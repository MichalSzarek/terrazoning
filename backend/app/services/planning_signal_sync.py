"""Sync normalized planning signals for future-buildability analysis."""

from __future__ import annotations

import asyncio
import html
from io import BytesIO
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx
from pypdf import PdfReader
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.gold import PlanningSignal, PlanningZone
from app.services.planning_signal_utils import normalize_designation_class, score_signal

logger = logging.getLogger(__name__)

_PLAN_TYPE_SIGNAL_KIND = {
    "pog": ("pog_zone", "formal_directional"),
    "studium": ("studium_zone", "formal_directional"),
}
_SIGNAL_SYNC_SOURCE_TYPES = ("planning_zone_passthrough", "html_index")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_ERROR_TEXT_RE = re.compile(r"błąd połączenia|blad polaczenia|connection error", flags=re.IGNORECASE)
_PLANNING_PAGE_SIGNAL_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"przystąp\w+ do sporządzenia planu ogólnego", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"wniosk\w+ do planu ogólnego", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"ogłoszenie\s*-\s*przystąpienie.*plan ogólny", flags=re.IGNORECASE | re.DOTALL),
        "formal_preparatory",
    ),
    (
        re.compile(r"obecnie trwa sporządzenie projektu planu", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"trwa sporządzenie projektu planu", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"rozpoczęto procedurę przetargową.*projektu planu ogólnego", flags=re.IGNORECASE | re.DOTALL),
        "formal_preparatory",
    ),
    (
        re.compile(r"wyłoniono wykonawcę projektu planu ogólnego", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"plan ogólny będzie podstawą", flags=re.IGNORECASE),
        "formal_directional",
    ),
    (
        re.compile(r"etap opiniowania i uzgadniania", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"opiniowanie i uzgadnianie planu ogólnego", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"opiniowanie z gminn\w+ komisj\w+ urbanistyczno-architektoniczn\w+", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"zbieranie wniosk\w+", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"zakończyliśmy etap zbierania uwag", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"rejestr\s*plan\w*\s*ogóln\w+", flags=re.IGNORECASE),
        "formal_directional",
    ),
    (
        re.compile(r"konsultacji społecznych planu ogólnego", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"przystąpienie do planu ogólnego", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
    (
        re.compile(r"plan ogólny w opracowaniu", flags=re.IGNORECASE),
        "formal_preparatory",
    ),
)
_STUDIUM_ROW_RE = re.compile(
    r"<tr[^>]*class=['\"][^'\"]*studium[^'\"]*['\"][^>]*>(.*?)</tr>",
    flags=re.IGNORECASE | re.DOTALL,
)
_TD_RE = re.compile(r"<td[^>]*class=['\"][^'\"]*cell[^'\"]*['\"][^>]*>(.*?)</td>", flags=re.IGNORECASE | re.DOTALL)
_HREF_RE = re.compile(r"href=['\"]([^'\"]+)['\"]", flags=re.IGNORECASE)
_POG_SECTION_RE = re.compile(
    r"POG - plan ogólny gminy</h2><div>(.*?)</div></div>",
    flags=re.IGNORECASE | re.DOTALL,
)
_HTML_INDEX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pl,en-US;q=0.9,en;q=0.8",
}


@dataclass(frozen=True)
class HtmlIndexSignalSource:
    teryt_gmina: str
    source_url: str
    label: str
    source_confidence: Decimal = Decimal("0.80")


@dataclass(frozen=True)
class HtmlIndexSourceProbeResult:
    teryt_gmina: str
    label: str
    source_url: str
    status: str
    signals_detected: int
    error: str | None = None


_HTML_INDEX_SIGNAL_REGISTRY: tuple[HtmlIndexSignalSource, ...] = (
    HtmlIndexSignalSource(
        teryt_gmina="2403052",
        source_url="https://chybie.e-mapa.net/wykazplanow/",
        label="Chybie Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2404042",
        source_url="https://kamienicapolska.e-mapa.net/wykazplanow/",
        label="Kamienica Polska Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2404042",
        source_url="https://bip.kamienicapolska.pl/artykul/plan-ogolny",
        label="Kamienica Polska Plan ogólny - BIP",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2404082",
        source_url="https://www.kruszyna.pl/plan-ogolny-gminy-kruszyna/",
        label="Kruszyna Plan ogólny gminy",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2409025",
        source_url="https://kozieglowy.e-mapa.net/wykazplanow/",
        label="Koziegłowy Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2409025",
        source_url="https://www.kozieglowy.pl/aktualnosci/4985",
        label="Koziegłowy Ogłoszenie o przystąpieniu do planu ogólnego",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2406092",
        source_url="https://www.bip.wreczyca-wielka.akcessnet.net/index.php?a=0&id=587&idg=3&x=65&y=10",
        label="Wręczyca Wielka Plan ogólny - BIP",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2406092",
        source_url="https://wreczyca-wielka.pl/aktualnosc-1167-ogloszenie.html",
        label="Wręczyca Wielka Ogłoszenie o planie ogólnym",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2414021",
        source_url="https://imielin.e-mapa.net/wykazplanow/",
        label="Imielin Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2414021",
        source_url="https://www.imielin.pl/pl/205/7551/plan-ogolny-miasta.html",
        label="Imielin Plan Ogólny Miasta - Aktualności",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2416085",
        source_url="https://szczekociny.e-mapa.net/wykazplanow/",
        label="Szczekociny Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2416085",
        source_url="https://szczekociny.geoportal-krajowy.pl/plan-ogolny",
        label="Szczekociny Plan ogólny - Geoportal Krajowy",
        source_confidence=Decimal("0.84"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2412014",
        source_url="https://bip.czerwionka-leszczyny.pl/informacje_urzedu/plan-ogolny-gminy-i-miasta-czerwionka-leszczyny-pog",
        label="Czerwionka-Leszczyny Plan ogólny - BIP",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2414042",
        source_url="https://bip.bojszowy.pl/pl/3144/0/plan-ogolny.html",
        label="Bojszowy Plan ogólny - BIP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2414042",
        source_url="https://bip.bojszowy.pl/pl/3145/26261/projekt-planu-ogolnego-etap-opiniowania-i-uzgadniania.html",
        label="Bojszowy Plan ogólny - etap opiniowania i uzgadniania",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowywisnicz.e-mapa.net/wykazplanow/",
        label="Nowy Wiśnicz Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowywisnicz.pl/aktualnosci/planowanie-przestrzenne/",
        label="Nowy Wiśnicz Planowanie przestrzenne",
        source_confidence=Decimal("0.84"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1218095",
        source_url="https://wadowice.e-mapa.net/wykazplanow/",
        label="Wadowice Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1218095",
        source_url="https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/plan-ogolny-gminy-wadowice/",
        label="Wadowice Plan ogólny gminy",
        source_confidence=Decimal("0.86"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1218095",
        source_url="https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/system-informacji-przestrzennej/",
        label="Wadowice Studium uwarunkowań",
        source_confidence=Decimal("0.84"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1217011",
        source_url="https://mzakopane.e-mapa.net/wykazplanow/",
        label="Zakopane Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1217011",
        source_url="https://www.zakopane.pl/zagospodarowanie-przestrzenne/plan-ogolny-gminy/",
        label="Zakopane Plan ogólny gminy",
        source_confidence=Decimal("0.86"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1213062",
        source_url="https://oswiecim.e-mapa.net/wykazplanow/",
        label="Gmina Oświęcim Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1213062",
        source_url="https://gminaoswiecim.pl/pl/1960/24294/sporzadzenie-planu-ogolnego-gminy-oswiecim-ogloszenie-wojta-gminy-oswiecim.html",
        label="Gmina Oświęcim plan ogólny - ogłoszenie",
        source_confidence=Decimal("0.86"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2404042",
        source_url="https://bip.kamienicapolska.pl/artykul/studium-uwarunkowan",
        label="Kamienica Polska Studium - obowiązujące",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2404042",
        source_url="https://bip.kamienicapolska.pl/artykul/opiniowanie-z-gminna-komisja-urbanistyczno-architektoniczna",
        label="Kamienica Polska Plan ogólny - GKUA",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2409055",
        source_url="https://www.zarki.bip.jur.pl/artykuly/6686",
        label="Żarki SIP - studium i planowanie przestrzenne",
        source_confidence=Decimal("0.84"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2409055",
        source_url="https://www.zarki.bip.jur.pl/kategorie/projekt_zmiany_studium_uwarunkowan_i_kierunkow",
        label="Żarki Projekt zmiany studium - BIP",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2409055",
        source_url="https://rastry.gison.pl/mpzp-public/zarki/uchwaly/U_2016_112_XVII_studium_tekst.pdf",
        label="Żarki Studium - tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
)
HTML_INDEX_SIGNAL_REGISTRY = _HTML_INDEX_SIGNAL_REGISTRY


@dataclass
class PlanningSignalSyncReport:
    scanned_zones: int = 0
    signals_created: int = 0
    signals_updated: int = 0
    duration_s: float = 0.0
    errors: list[str] = field(default_factory=list)


class PlanningSignalSync:
    """Materialize future-buildability signals from geometry-backed planning zones."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def sync(self, *, teryt_gmina: Optional[str] = None) -> PlanningSignalSyncReport:
        started = asyncio.get_event_loop().time()
        report = PlanningSignalSyncReport()

        stmt = select(PlanningZone).where(PlanningZone.plan_type.in_(tuple(_PLAN_TYPE_SIGNAL_KIND)))
        if teryt_gmina:
            stmt = stmt.where(PlanningZone.teryt_gmina == teryt_gmina)
        zones = (await self.db.execute(stmt)).scalars().all()
        report.scanned_zones = len(zones)

        if teryt_gmina:
            await self.db.execute(
                delete(PlanningSignal).where(
                    PlanningSignal.teryt_gmina == teryt_gmina,
                    PlanningSignal.source_type.in_(_SIGNAL_SYNC_SOURCE_TYPES),
                )
            )
        else:
            await self.db.execute(
                delete(PlanningSignal).where(
                    PlanningSignal.source_type.in_(_SIGNAL_SYNC_SOURCE_TYPES),
                )
            )

        for zone in zones:
            signal_kind, signal_status = _PLAN_TYPE_SIGNAL_KIND[zone.plan_type]
            designation_normalized = normalize_designation_class(
                zone.przeznaczenie,
                zone.przeznaczenie_opis,
            )
            legal_weight = score_signal(
                signal_kind=signal_kind,
                designation_normalized=designation_normalized,
                signal_status=signal_status,
            )
            evidence_chain = [
                {
                    "step": "planning_zone",
                    "ref": str(zone.id),
                    "plan_type": zone.plan_type,
                    "plan_name": zone.plan_name,
                    "designation_raw": zone.przeznaczenie,
                }
            ]
            signal = PlanningSignal(
                teryt_gmina=zone.teryt_gmina,
                signal_kind=signal_kind,
                signal_status=signal_status,
                designation_raw=zone.przeznaczenie,
                designation_normalized=designation_normalized,
                description=zone.przeznaczenie_opis,
                plan_name=zone.plan_name,
                uchwala_nr=zone.uchwala_nr,
                effective_date=zone.plan_effective_date,
                source_url=zone.source_wfs_url,
                source_type="planning_zone_passthrough",
                source_confidence=Decimal("1.00"),
                legal_weight=legal_weight,
                geom=zone.geom,
                evidence_chain=evidence_chain,
                updated_at=datetime.now(timezone.utc),
            )
            self.db.add(signal)
            report.signals_created += 1

        for source in _HTML_INDEX_SIGNAL_REGISTRY:
            if teryt_gmina and source.teryt_gmina != teryt_gmina:
                continue
            try:
                html_signals = await self._fetch_html_index_signals(source)
            except Exception as exc:
                report.errors.append(f"{source.teryt_gmina}: {exc}")
                logger.warning("[PlanningSignalSync] html_index failed for %s: %s", source.teryt_gmina, exc)
                continue
            for signal in html_signals:
                self.db.add(signal)
                report.signals_created += 1

        await self.db.commit()
        report.duration_s = round(asyncio.get_event_loop().time() - started, 2)
        logger.info(
            "[PlanningSignalSync] scanned=%d created=%d updated=%d duration=%.2fs",
            report.scanned_zones,
            report.signals_created,
            report.signals_updated,
            report.duration_s,
        )
        return report

    async def _fetch_html_index_signals(
        self,
        source: HtmlIndexSignalSource,
    ) -> list[PlanningSignal]:
        headers = dict(_HTML_INDEX_HEADERS)
        headers["Referer"] = re.sub(r"/wykazplanow/?$", "/", source.source_url)
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            response = await client.get(source.source_url, headers=headers)
            response.raise_for_status()
        source_type = _detect_source_type(source.source_url, response.headers.get("content-type"))
        if source_type == "pdf":
            page = _extract_pdf_text(response.content)
        else:
            page = response.text
            html_index_error = _detect_html_index_error(page)
            if html_index_error:
                raise ValueError(html_index_error)

        signals: list[PlanningSignal] = []
        studium_match = _STUDIUM_ROW_RE.search(page) if source_type == "html_index" else None
        if studium_match:
            cells = _TD_RE.findall(studium_match.group(1))
            if len(cells) >= 7:
                designation_raw = _clean_html_fragment(cells[0]) or "SUIKZP"
                title = _clean_html_fragment(cells[1])
                effective_date = _parse_signal_date(_clean_html_fragment(cells[4]))
                uchwala_nr = _clean_html_fragment(cells[6])
                source_url = _resolve_relative_href(source.source_url, cells[8] if len(cells) > 8 else "") or source.source_url
                evidence_chain = [
                    {
                        "step": "html_index",
                        "ref": source.source_url,
                        "designation_raw": designation_raw,
                        "title": title,
                        "label": source.label,
                    }
                ]
                designation_normalized = normalize_designation_class(designation_raw, title)
                signals.append(
                    PlanningSignal(
                        teryt_gmina=source.teryt_gmina,
                        signal_kind="planning_resolution",
                        signal_status="formal_directional",
                        designation_raw=designation_raw,
                        designation_normalized=designation_normalized,
                        description=title,
                        plan_name=title,
                        uchwala_nr=uchwala_nr or None,
                        effective_date=effective_date,
                        source_url=source_url,
                        source_type=source_type,
                        source_confidence=source.source_confidence,
                        legal_weight=score_signal(
                            signal_kind="planning_resolution",
                            designation_normalized=designation_normalized,
                            signal_status="formal_directional",
                        ),
                        geom=None,
                        evidence_chain=evidence_chain,
                        updated_at=datetime.now(timezone.utc),
                    )
                )

        pog_match = _POG_SECTION_RE.search(page) if source_type == "html_index" else None
        if pog_match and "w opracowaniu" in pog_match.group(1).lower():
            description = _clean_html_fragment(pog_match.group(1))
            pog_url = _resolve_relative_href(source.source_url, pog_match.group(1)) or source.source_url
            signals.append(
                PlanningSignal(
                    teryt_gmina=source.teryt_gmina,
                    signal_kind="planning_resolution",
                    signal_status="formal_preparatory",
                    designation_raw="POG",
                    designation_normalized="unknown",
                    description=description,
                    plan_name=f"POG {source.teryt_gmina}",
                    uchwala_nr=None,
                    effective_date=None,
                    source_url=pog_url,
                    source_type=source_type,
                    source_confidence=Decimal("0.70"),
                    legal_weight=score_signal(
                        signal_kind="planning_resolution",
                        designation_normalized="unknown",
                        signal_status="formal_preparatory",
                    ),
                    geom=None,
                    evidence_chain=[
                        {
                            "step": "html_index",
                            "ref": source.source_url,
                            "designation_raw": "POG",
                            "title": description,
                            "label": source.label,
                        }
                    ],
                    updated_at=datetime.now(timezone.utc),
                    )
            )

        if not signals:
            studium_signal = _parse_studium_page_signal(
                page,
                source,
                source_type=source_type,
            )
            if studium_signal is not None:
                signals.append(studium_signal)
            else:
                generic_signal = _parse_generic_planning_page_signal(
                    page,
                    source,
                    source_type=source_type,
                )
                if generic_signal is not None:
                    signals.append(generic_signal)

        return signals


async def probe_html_index_source(
    source: HtmlIndexSignalSource,
) -> HtmlIndexSourceProbeResult:
    try:
        signals = await PlanningSignalSync.__new__(PlanningSignalSync)._fetch_html_index_signals(source)
    except Exception as exc:
        return HtmlIndexSourceProbeResult(
            teryt_gmina=source.teryt_gmina,
            label=source.label,
            source_url=source.source_url,
            status="upstream_broken",
            signals_detected=0,
            error=str(exc),
        )

    status = "live" if signals else "partial"
    return HtmlIndexSourceProbeResult(
        teryt_gmina=source.teryt_gmina,
        label=source.label,
        source_url=source.source_url,
        status=status,
        signals_detected=len(signals),
        error=None,
    )


async def probe_html_index_registry(
    *,
    teryt_gmina: Optional[str] = None,
) -> list[HtmlIndexSourceProbeResult]:
    results: list[HtmlIndexSourceProbeResult] = []
    for source in _HTML_INDEX_SIGNAL_REGISTRY:
        if teryt_gmina and source.teryt_gmina != teryt_gmina:
            continue
        results.append(await probe_html_index_source(source))
    return results


def _clean_html_fragment(value: str | None) -> str:
    if not value:
        return ""
    cleaned = _HREF_RE.sub("", value)
    cleaned = _HTML_TAG_RE.sub(" ", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = cleaned.replace("\xa0", " ")
    return " ".join(cleaned.split()).strip()


def _detect_html_index_error(page: str | None) -> str | None:
    text = _clean_html_fragment(page)
    if not text:
        return "HTML index returned an empty response body"
    if _HTML_ERROR_TEXT_RE.search(text):
        return f"HTML index returned an operator error page: {text[:160]}"
    return None


def _detect_source_type(source_url: str, content_type: str | None) -> str:
    lowered_url = source_url.lower()
    lowered_content_type = (content_type or "").lower()
    if lowered_url.endswith(".pdf") or "application/pdf" in lowered_content_type:
        return "pdf"
    return "html_index"


def _extract_pdf_text(payload: bytes) -> str:
    reader = PdfReader(BytesIO(payload))
    chunks: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text:
            chunks.append(text)
    return "\n".join(chunks).strip()


def _parse_generic_planning_page_signal(
    page: str,
    source: HtmlIndexSignalSource,
    *,
    source_type: str = "html_index",
) -> PlanningSignal | None:
    cleaned_page = _clean_html_fragment(page)
    if not cleaned_page:
        return None
    lowered = cleaned_page.lower()

    direct_match: tuple[str, str, str] | None = None
    if (
        "miejscowego planu zagospodarowania przestrzennego" in lowered
        and ("przystąp" in lowered or "projekt" in lowered)
    ):
        direct_match = ("mpzp_project", "MPZP", f"MPZP {source.teryt_gmina}")
    elif (
        "studium uwarunkowań i kierunków zagospodarowania przestrzennego" in lowered
        and ("zmian" in lowered or "projekt" in lowered or "uzgadnian" in lowered)
    ):
        direct_match = ("planning_resolution", "SUiKZP", f"SUiKZP {source.teryt_gmina}")

    if direct_match is not None:
        signal_kind, designation_raw, plan_name = direct_match
        designation_normalized = normalize_designation_class(designation_raw, cleaned_page)
        return PlanningSignal(
            teryt_gmina=source.teryt_gmina,
            signal_kind=signal_kind,
            signal_status="formal_preparatory",
            designation_raw=designation_raw,
            designation_normalized=designation_normalized,
            description=cleaned_page[:240],
            plan_name=plan_name,
            uchwala_nr=None,
            effective_date=None,
            source_url=source.source_url,
            source_type=source_type,
            source_confidence=source.source_confidence,
            legal_weight=score_signal(
                signal_kind=signal_kind,
                designation_normalized=designation_normalized,
                signal_status="formal_preparatory",
            ),
            geom=None,
            evidence_chain=[
                {
                    "step": "html_index",
                    "ref": source.source_url,
                    "designation_raw": designation_raw,
                    "title": cleaned_page[:240],
                    "label": source.label,
                }
            ],
            updated_at=datetime.now(timezone.utc),
        )

    for pattern, signal_status in _PLANNING_PAGE_SIGNAL_PATTERNS:
        match = pattern.search(cleaned_page)
        if not match:
            continue
        snippet = _extract_context_snippet(cleaned_page, match.start(), match.end())
        signal_kind = "planning_resolution"
        designation_raw = "POG"
        plan_name = f"POG {source.teryt_gmina}"

        if (
            "miejscowego planu zagospodarowania przestrzennego" in lowered
            and ("przystąp" in lowered or "projekt" in lowered)
        ):
            signal_kind = "mpzp_project"
            designation_raw = "MPZP"
            plan_name = f"MPZP {source.teryt_gmina}"
        elif "studium uwarunkowań i kierunków zagospodarowania przestrzennego" in lowered:
            designation_raw = "SUiKZP"
            plan_name = f"SUiKZP {source.teryt_gmina}"

        designation_normalized = normalize_designation_class(designation_raw, snippet)
        return PlanningSignal(
            teryt_gmina=source.teryt_gmina,
            signal_kind=signal_kind,
            signal_status=signal_status,
            designation_raw=designation_raw,
            designation_normalized=designation_normalized,
            description=snippet,
            plan_name=plan_name,
            uchwala_nr=None,
            effective_date=None,
            source_url=source.source_url,
            source_type=source_type,
            source_confidence=source.source_confidence,
            legal_weight=score_signal(
                signal_kind=signal_kind,
                designation_normalized=designation_normalized,
                signal_status=signal_status,
            ),
            geom=None,
            evidence_chain=[
                {
                    "step": "html_index",
                    "ref": source.source_url,
                    "designation_raw": designation_raw,
                    "title": snippet,
                    "label": source.label,
                }
            ],
            updated_at=datetime.now(timezone.utc),
        )

    return None


def _parse_studium_page_signal(
    page: str,
    source: HtmlIndexSignalSource,
    *,
    source_type: str = "html_index",
) -> PlanningSignal | None:
    cleaned_page = _clean_html_fragment(page)
    if not cleaned_page:
        return None
    if not re.search(
        r"obowiązujące studium uwarunkowań i kierunków zagospodarowania przestrzennego",
        cleaned_page,
        flags=re.IGNORECASE,
    ):
        return None

    match = re.search(
        r"studium uwarunkowań i kierunków zagospodarowania przestrzennego.*?(?:gminy\s+)?[A-ZĄĆĘŁŃÓŚŹŻ][^.;\n]{0,80}",
        cleaned_page,
        flags=re.IGNORECASE,
    )
    snippet = _extract_context_snippet(cleaned_page, match.start(), match.end()) if match else cleaned_page[:240]

    return PlanningSignal(
        teryt_gmina=source.teryt_gmina,
        signal_kind="planning_resolution",
        signal_status="formal_directional",
        designation_raw="SUiKZP",
        designation_normalized="unknown",
        description=snippet,
        plan_name=f"SUiKZP {source.teryt_gmina}",
        uchwala_nr=None,
        effective_date=None,
        source_url=source.source_url,
        source_type=source_type,
        source_confidence=source.source_confidence,
        legal_weight=score_signal(
            signal_kind="planning_resolution",
            designation_normalized="unknown",
            signal_status="formal_directional",
        ),
        geom=None,
        evidence_chain=[
            {
                "step": "html_index",
                "ref": source.source_url,
                "designation_raw": "SUiKZP",
                "title": snippet,
                "label": source.label,
            }
        ],
        updated_at=datetime.now(timezone.utc),
    )


def _extract_context_snippet(text: str, start: int, end: int, *, radius: int = 180) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    snippet = text[left:right]
    return " ".join(snippet.split()).strip()


def _resolve_relative_href(base_url: str, html_fragment: str) -> str | None:
    match = _HREF_RE.search(html_fragment or "")
    if not match:
        return None
    href = html.unescape(match.group(1))
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return str(httpx.URL(base_url).join(href))


def _parse_signal_date(value: str | None):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


async def run_planning_signal_sync(*, teryt_gmina: Optional[str] = None) -> PlanningSignalSyncReport:
    from app.core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        service = PlanningSignalSync(db)
        return await service.sync(teryt_gmina=teryt_gmina)
