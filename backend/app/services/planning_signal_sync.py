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
from app.services.planning_signal_utils import POSITIVE_DESIGNATIONS, normalize_designation_class, score_signal

logger = logging.getLogger(__name__)

_PLAN_TYPE_SIGNAL_KIND = {
    "pog": ("pog_zone", "formal_directional"),
    "studium": ("studium_zone", "formal_directional"),
}
_SIGNAL_SYNC_SOURCE_TYPES = ("planning_zone_passthrough", "html_index", "pdf")
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


def _is_ssl_certificate_error(exc: Exception) -> bool:
    message = str(exc).upper()
    return "CERTIFICATE_VERIFY_FAILED" in message or "CERTIFICATE VERIFY FAILED" in message


async def _fetch_source_response(
    source_url: str,
    *,
    headers: dict[str, str],
) -> httpx.Response:
    request_kwargs = {
        "timeout": httpx.Timeout(20.0),
        "follow_redirects": True,
    }
    try:
        async with httpx.AsyncClient(**request_kwargs) as client:
            response = await client.get(source_url, headers=headers)
            response.raise_for_status()
            return response
    except httpx.ConnectError as exc:
        if not _is_ssl_certificate_error(exc):
            raise
        logger.warning(
            "[PlanningSignalSync] retrying %s with TLS verification disabled due to certificate error",
            source_url,
        )
        async with httpx.AsyncClient(verify=False, **request_kwargs) as client:
            response = await client.get(source_url, headers=headers)
            response.raise_for_status()
            return response


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
        teryt_gmina="2406092",
        source_url="https://www.wreczyca-wielka.pl/aktualnosc-281-obwieszczenie_o_przystapieniu_do.html",
        label="Wręczyca Wielka Obwieszczenie o przystąpieniu do planu ogólnego",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2406092",
        source_url="https://www.bip.wreczyca-wielka.akcessnet.net/upload/20180119081806odmqcs0uipum.pdf",
        label="Wręczyca Wielka Studium 2017 PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2406092",
        source_url="https://www.bip.wreczyca-wielka.akcessnet.net/upload/20170607123606h7nz89qvgekf.pdf",
        label="Wręczyca Wielka Studium 2016 PDF",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2406092",
        source_url="https://www.bip.wreczyca-wielka.akcessnet.net/upload/plik%2C20250828213836%2Cuzasadnienie_do_planu_ogolnego_gminy_wreczyca_wielka.pdf",
        label="Wręczyca Wielka uzasadnienie planu ogólnego PDF",
        source_confidence=Decimal("0.92"),
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
        teryt_gmina="2414021",
        source_url="https://bip.imielin.pl/pl/2350/0/plan-ogolny.html",
        label="Imielin Plan ogólny - BIP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2414021",
        source_url="https://bip.imielin.pl/mfiles/2369/28/0/z/uzasadnienie-do-planu-og-lnego.pdf",
        label="Imielin POG uzasadnienie PDF",
        source_confidence=Decimal("0.92"),
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
        teryt_gmina="2416085",
        source_url="https://mapa.inspire-hub.pl/upload/141_XXI_2016_SUiKZP_tekst__szczekociny.pdf?action_type=3",
        label="Szczekociny Studium tekst uchwały PDF",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2416085",
        source_url="https://bip.szczekociny.pl/res/serwisy/pliki/13447905?version=1.0",
        label="Szczekociny Studium tekst jednolity - tom I",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2416085",
        source_url="https://bip.szczekociny.pl/res/serwisy/pliki/13447918?version=1.0",
        label="Szczekociny Studium tekst jednolity - tom II",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2416085",
        source_url="https://bip.szczekociny.pl/res/serwisy/pliki/42216826?version=1.0",
        label="Szczekociny POG GML",
        source_confidence=Decimal("0.94"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2416085",
        source_url="https://bip.szczekociny.pl/res/serwisy/pliki/42216838?version=1.0",
        label="Szczekociny POG uzasadnienie PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2412014",
        source_url="https://bip.czerwionka-leszczyny.pl/informacje_urzedu/plan-ogolny-gminy-i-miasta-czerwionka-leszczyny-pog",
        label="Czerwionka-Leszczyny Plan ogólny - BIP",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2412014",
        source_url="https://bip.czerwionka-leszczyny.pl/pliki/Uzasadnienie-POG-CZERWIONKA-MARZEC-2026,36998.pdf",
        label="Czerwionka-Leszczyny POG - uzasadnienie",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2412014",
        source_url="https://bip.czerwionka-leszczyny.pl/pliki/1POG-Czerwionka-Leszczyny-30-03-2026,37000.gml",
        label="Czerwionka-Leszczyny POG - GML projekt",
        source_confidence=Decimal("0.94"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2405011",
        source_url="https://www.knurow.pl/miasto-knurow/ogloszenia-urzedu/OBWIESZCZENIE-PREZYDENTA-MIASTA-KNUROW-z-dnia-7-marca-2024-r/idn:4390",
        label="Knurów Obwieszczenie o zmianie MPZP Szpitalna i 26 Stycznia",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2405011",
        source_url="https://www.knurow.pl/download/Uzasadnienie,812.pdf",
        label="Knurów Uzasadnienie zmiany MPZP Szpitalna i 26 Stycznia",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2405011",
        source_url="https://www.knurow.pl/miasto-knurow/ogloszenia-urzedu/Obwieszczenie-Prezydenta-Miasta-Knurow/idn:5841",
        label="Knurów Obwieszczenie o przyjęciu MPZP Szpitalna",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2405011",
        source_url="https://www.knurow.pl/download/Uchwala-XV_165_2025-RM-Knurow,1182.pdf",
        label="Knurów Uchwała XV/165/2025 MPZP Szpitalna",
        source_confidence=Decimal("0.94"),
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
        teryt_gmina="2469011",
        source_url="https://www.katowice.eu/plan-ogolny",
        label="Katowice Plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2469011",
        source_url="https://bip.katowice.eu/Lists/Dokumenty/Attachments/150551/Uzasadnienie%20do%20projektu%20POG%20Katowice%20-%20Etap%20opiniowania%20i%20uzgodnie%C5%84.pdf",
        label="Katowice POG uzasadnienie PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2469011",
        source_url="https://bip.katowice.eu/PublishingImages/Planowanie%20Przestrzenne/tekst%20Studium%20cz%C4%99%C5%9B%C4%87%201%20-%20Uwarunkowania%20zagospodarowania%20przestrzennego.pdf",
        label="Katowice Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowywisnicz.e-mapa.net/wykazplanow/",
        label="Nowy Wiśnicz Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1202024",
        source_url="https://www.brzesko.pl/wpis/87444%2Copracowanie-planu-ogolnego-zagospodarowania-przestrzennego-dla-gminy-brzesko",
        label="Brzesko Opracowanie planu ogólnego",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1202024",
        source_url="https://brzesko.geoportal-krajowy.pl/mpzp",
        label="Brzesko Geoportal MPZP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1202024",
        source_url="https://www.brzesko.pl/artykul/222%2Cplanowanie-przestrzenne",
        label="Brzesko Planowanie przestrzenne",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1805042",
        source_url="https://jaslo.e-mapa.net/wykazplanow/",
        label="Gmina Jasło Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1805042",
        source_url="https://rejestrplanowogolnych.pl/?teryt=180504_2",
        label="Gmina Jasło Rejestr planu ogólnego",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1805042",
        source_url="https://rastry.gison.pl/mpzp-public/jaslogmina/uchwaly/U_2018_433_LXVIII_studium.pdf",
        label="Gmina Jasło Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1808042",
        source_url="https://uglezajsk.bip.gov.pl/mpzp-giedlarowa/o-b-w-i-e-s-z-c-z-e-n-i-e-wojta-gminy-lezajsk-z-dnia-27-02-2024-o-przystapieniu-do-sporzadzania-miejscowego-planu-zagospodarowania-przestrzennego-terenu-wsi-giedlarowa.html",
        label="Giedlarowa Obwieszczenie o przystąpieniu do MPZP",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1808042",
        source_url="https://uglezajsk.bip.gov.pl/planowanie-przestrzenne/zbiory-danych-przestrzennych/zbior-app-dla-mpz/zbior-danych-mpzp-makemaker.html",
        label="Gmina Leżajsk APP MPZP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1808042",
        source_url="https://uglezajsk.bip.gov.pl/planowanie-przestrzenne/zbiory-danych-przestrzennych/zbior-app-dla-studium/zbior-danych-app-dla-studium-makemaker.html",
        label="Gmina Leżajsk APP Studium",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1816035",
        source_url="https://bip.boguchwala.pl/pl/404-menu-tematyczne/12304-planowanie-przestrzenne.html",
        label="Boguchwała Planowanie przestrzenne - BIP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1816035",
        source_url="https://boguchwala.geoportal-krajowy.pl/plan-ogolny",
        label="Boguchwała Geoportal plan ogólny",
        source_confidence=Decimal("0.86"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1816035",
        source_url="https://rastry.gison.pl/mpzp-public/boguchwala/uchwaly/U_2020_354_XXIX_studium_tekst.pdf",
        label="Boguchwała Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1816035",
        source_url="https://rastry.gison.pl/mpzp-public/boguchwala/uchwaly/U_2019_218_XV.pdf",
        label="Boguchwała MPZP tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1804082",
        source_url="https://www.ugradymno.pl/asp/plan-ogolny-informacje%2C1%2Cartykul%2C1%2C1625",
        label="Gmina Radymno Plan ogólny - informacje",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1805112",
        source_url="https://www.ugradymno.pl/asp/core/pdf.asp?akcja=artykul&artykul=1551&menu=1",
        label="Gmina Radymno Plan ogólny - PDF artykułu",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1804082",
        source_url="https://radymno.geoportal-krajowy.pl/plan-ogolny",
        label="Gmina Radymno Geoportal plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1804082",
        source_url="https://radymno.geoportal-krajowy.pl/mpzp",
        label="Gmina Radymno Geoportal MPZP",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1805112",
        source_url="https://bip.tarnowiec.eu/planowanie-przestrzenne/238",
        label="Tarnowiec Planowanie przestrzenne - BIP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1805112",
        source_url="https://bip.tarnowiec.eu/projekty-mpzp/290",
        label="Tarnowiec Projekty MPZP - BIP",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1805112",
        source_url="https://tarnowiec.eu/aktualnosc-4123-przystapienie_do_sporzadzenia_planu.html",
        label="Tarnowiec Przystąpienie do sporządzenia planu ogólnego",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803042",
        source_url="https://gminadebica.e-mapa.net/wykazplanow/",
        label="Gmina Dębica Rejestr urbanistyczny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803052",
        source_url="https://jodlowa.e-mapa.net/wykazplanow/",
        label="Jodłowa Rejestr urbanistyczny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1807025",
        source_url="https://dukla.e-mapa.net/wykazplanow/",
        label="Dukla Rejestr urbanistyczny",
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1807025",
        source_url="https://dukla.geoportal-krajowy.pl/plan-ogolny",
        label="Dukla Geoportal plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1807025",
        source_url="https://www.dukla.pl/pl/dla-mieszkancow/mapy-i-plany-79/wnioski-do-planu-ogolnego-226",
        label="Dukla Wnioski do planu ogólnego",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1807025",
        source_url="https://www.dukla.pl/files/_source/2025/01/ogloszenie%20na%20BIP%20i%20na%20strone%20gminy.pdf",
        label="Dukla Ogłoszenie planu ogólnego PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1809054",
        source_url="https://narol.geoportal-krajowy.pl/plan-ogolny",
        label="Narol Geoportal plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1809054",
        source_url="https://narol.geoportal-krajowy.pl/mpzp",
        label="Narol Geoportal MPZP",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803042",
        source_url="https://debica.geoportal-krajowy.pl/",
        label="Dębica Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803042",
        source_url="https://debica.geoportal-krajowy.pl/mpzp",
        label="Dębica Geoportal MPZP registry",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803052",
        source_url="https://jodlowa.geoportal-krajowy.pl/",
        label="Jodłowa Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803052",
        source_url="https://jodlowa.geoportal-krajowy.pl/plan-ogolny",
        label="Jodłowa Geoportal plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803065",
        source_url="https://pilzno.geoportal-krajowy.pl/",
        label="Pilzno Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803065",
        source_url="https://pilzno.geoportal-krajowy.pl/plan-ogolny",
        label="Pilzno Geoportal plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1803065",
        source_url="https://pilzno.geoportal-krajowy.pl/mpzp",
        label="Pilzno Geoportal MPZP registry",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1811032",
        source_url="https://czermin-mielecki.geoportal-krajowy.pl/",
        label="Czermin Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1811032",
        source_url="https://czermin-mielecki.geoportal-krajowy.pl/plan-ogolny",
        label="Czermin Geoportal plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1816065",
        source_url="https://glogow-malopolski.geoportal-krajowy.pl/",
        label="Głogów Małopolski Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1816065",
        source_url="https://glogow-malopolski.geoportal-krajowy.pl/mpzp",
        label="Głogów Małopolski Geoportal MPZP registry",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1815012",
        source_url="https://iwierzyce.e-mapa.net/wykazplanow/",
        label="Iwierzyce Rejestr urbanistyczny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1819032",
        source_url="https://niebylec.geoportal-krajowy.pl/",
        label="Niebylec Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1819032",
        source_url="https://niebylec.geoportal-krajowy.pl/plan-ogolny",
        label="Niebylec Geoportal plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1819045",
        source_url="https://strzyzow.geoportal-krajowy.pl/",
        label="Strzyżów Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1819045",
        source_url="https://strzyzow.geoportal-krajowy.pl/mpzp",
        label="Strzyżów Geoportal MPZP registry",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1820032",
        source_url="https://grebow.geoportal-krajowy.pl/",
        label="Grębów Geoportal landing page",
        source_confidence=Decimal("0.82"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1820032",
        source_url="https://grebow.geoportal-krajowy.pl/mpzp",
        label="Grębów Geoportal MPZP registry",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowywisnicz.pl/aktualnosci/planowanie-przestrzenne/",
        label="Nowy Wiśnicz Planowanie przestrzenne",
        source_confidence=Decimal("0.84"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowywisnicz.e-mapa.net/implementation/nowywisnicz/pln/pelna_tresc/000.pdf",
        label="Nowy Wiśnicz Studium tekst uchwały PDF",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206152",
        source_url="https://www.wielka-wies.pl/o-gminie/aktualnosci/ogloszenie-plan-ogolny/",
        label="Wielka Wieś Ogłoszenie plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206152",
        source_url="https://wielka-wies.geoportal-krajowy.pl/plan-ogolny",
        label="Wielka Wieś Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206152",
        source_url="https://old.wielka-wies.pl/media/191132/zal-1-wielka-wies-studium-tekst-ujednolicony.pdf",
        label="Wielka Wieś Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1203034",
        source_url="https://www.chrzanow.pl/gmina/planowanie-przestrzenne/plan-ogolny",
        label="Chrzanów Plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1203034",
        source_url="https://chrzanow.geoportal-krajowy.pl/plan-ogolny",
        label="Chrzanów Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1203034",
        source_url="https://www.chrzanow.pl/gmina/planowanie-przestrzenne/plany-zagospodarowania---projekty",
        label="Chrzanów Plany zagospodarowania - projekty",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1203034",
        source_url="https://www.chrzanow.pl/aktualnosci/plan-ogolny-gminy-chrzanow--mozna-skladac-wnioski%2C2737",
        label="Chrzanów Plan ogólny - składanie wniosków",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1203034",
        source_url="https://www.chrzanow.pl/gmina/planowanie-przestrzenne/plany-zagospodarowania---projekty/projekt-zmiany-mpzp-dla-terenu-gorniczego-babice-i",
        label="Chrzanów Projekt zmiany MPZP Babice I",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1203034",
        source_url="https://www.chrzanow.pl/storage/file/core_files/2024/3/18/52f16e1503ab2ccac845e1e91f12e2aa/Protok%C3%B3%C5%82%20z%20dyskusji%20publicznej_2024.pdf",
        label="Chrzanów Protokół dyskusji publicznej PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1205092",
        source_url="https://www.sekowa.pl/strefa_mieszkanca/ogloszenie-o-zamieszczeniu-danych-o-projekcie-zmiany-miejscowego-planu-zagospodarowania-przestrzennego-gminy-sekowa/",
        label="Sękowa Projekt zmiany MPZP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1205092",
        source_url="https://www.sekowa.pl/strefa_mieszkanca/ogloszenie-wojta-gminy-sekowa-z-dnia-30-stycznia-2025-r-o-przystapieniu-do-sporzadzenia-zmiany-miejscowego-planu-zagospodarowania-przestrzennego-gminy-sekowa/",
        label="Sękowa Obwieszczenie o zmianie MPZP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1205092",
        source_url="https://www.sekowa.pl/plan-zagospodarowania-przestrzennego/",
        label="Sękowa Plan zagospodarowania przestrzennego",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1210062",
        source_url="https://rastry.gison.pl/mpzp-public/korzenna/uchwaly/U_2018_375_XXXIV_studium_tekst.pdf",
        label="Korzenna Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1210062",
        source_url="https://www.korzenna.pl/plan-ogolny-zamiast-studium-uwarunkowan-wnioski-do-27-grudnia/",
        label="Korzenna Plan ogólny zamiast studium",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1210062",
        source_url="https://www.korzenna.pl/setki-wnioskow-do-planu-gminy/",
        label="Korzenna Setki wniosków do planu gminy",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1210062",
        source_url="https://www.korzenna.pl/blog/2024/12/13/informacja-o-nieobowiazywaniu-zapisow-studium-w-planie-ogolnym-gminy-korzenna/",
        label="Korzenna Informacja o nieobowiązywaniu studium",
        source_confidence=Decimal("0.86"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206022",
        source_url="https://igolomia-wawrzenczyce.geoportal-krajowy.pl/plan-ogolny",
        label="Igołomia-Wawrzeńczyce Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206032",
        source_url="https://www.iwanowice.pl/dla-mieszkanca/plan-ogolny-gminy-iwanowice/",
        label="Iwanowice Plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206032",
        source_url="https://www.iwanowice.pl/zapuveer/2024/09/SUiKZP_Iwanowice_Zalacznik_2_Ustalenia_2024-09.pdf.pdf",
        label="Iwanowice Studium ustalenia PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206032",
        source_url="https://www.iwanowice.pl/zapuveer/2024/09/SUiKZP_Iwanowice_Zalacznik_1_Uwarunkowania_2024-09.pdf.pdf",
        label="Iwanowice Studium uwarunkowania PDF",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206032",
        source_url="https://iwanowice.geoportal-krajowy.pl/plan-ogolny",
        label="Iwanowice Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206032",
        source_url="https://www.iwanowice.pl/ogloszenie-wojta-iwanowice-projekt-zmiany-studium/",
        label="Iwanowice ogłoszenie o projekcie zmiany studium",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206032",
        source_url="https://iwanowice.pl/wp-content/uploads/2021/11/MPZP_TEKS.pdf",
        label="Iwanowice MPZP tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206105",
        source_url="https://skala.pl/studium/",
        label="Skała Studium",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206105",
        source_url="https://skala.pl/wp-content/uploads/2024/01/13_KIERUNKI_SKALA_wylozenie.pdf",
        label="Skała Kierunki studium PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206105",
        source_url="https://skala.geoportal-krajowy.pl/plan-ogolny",
        label="Skała Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206105",
        source_url="https://skala.pl/dokumenty/plan-zagospodarowania/",
        label="Skała Plan zagospodarowania",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206105",
        source_url="https://skala.pl/obwieszczenie-burmistrza-miasta-i-gminy-skala-o-wylozeniu-do-publicznego-wgladu-projektu-studium-uwarunkowan-i-kierunkow-zagospodarowania-przestrzennego-21/",
        label="Skała Obwieszczenie o wyłożeniu studium",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206114",
        source_url="https://www.gminaskawina.pl/mieszkancy/informacje-praktyczne/miejscowy-plan-zagospodarowania-przestrzennego/studium-uwarunkowan-i-kierunkow-zagospodarowania-przestrzennego-gminy-skawina",
        label="Skawina Studium",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206114",
        source_url="https://rastry.gison.pl/mpzp-public/skawina_wylozenie/uchwaly/studium_wylozenie_kierunki.pdf",
        label="Skawina Studium kierunki PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206114",
        source_url="https://skawina.geoportal-krajowy.pl/plan-ogolny",
        label="Skawina Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206114",
        source_url="https://www.gminaskawina.pl/assets/skawina/media/files/8613e3e7-8b67-4c9f-87de-765280dc4049/uchwala-nr-ii-16-24-rady-miejskiej-w-skawinie.pdf",
        label="Skawina Uchwała MPZP PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206114",
        source_url="https://www.gminaskawina.pl/mieszkancy/informacje-praktyczne/miejscowy-plan-zagospodarowania-przestrzennego/aktualnosci-gp/2025/ogloszenie-burmistrza-miasta-i-gminy-skawina-za-dnia-13-czerwca-2025-r",
        label="Skawina Ogłoszenie Burmistrza 13 czerwca 2025",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206114",
        source_url="https://www.gminaskawina.pl/assets/skawina/media/files/f9ec549d-02d6-4836-9b1a-cef2d2e9de25/projekt-zmiany-mpzp-miasta-skawina-kdd-wylozenie-20-11-2023.pdf",
        label="Skawina Projekt zmiany MPZP KDD PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206162",
        source_url="https://zabierzow.geoportal-krajowy.pl/plan-ogolny",
        label="Zabierzów Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1206162",
        source_url="https://zabierzow.org.pl/572-plan-ogolny.html",
        label="Zabierzów Plan ogólny - portal gminy",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1208045",
        source_url="https://www.ksiazwielki.eu/index.php/dla-mieszkanca/ogloszenia-i-komunikaty/528-ogloszenie-o-rozpoczeciu-konsultacji-spolecznych-projektu-planu-ogolnego-miasta-i-gminy-ksiaz-wielki",
        label="Książ Wielki konsultacje planu ogólnego",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1208045",
        source_url="https://e-mapa.net/plan_ogolny/120804-ksiaz-wielki",
        label="Książ Wielki e-mapa plan ogólny",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1211092",
        source_url="https://www.ugnowytarg.pl/strefy/planowanie-przestrzenne-i-budownictwo/plan-ogolny-gminy-nowy-targ",
        label="Gmina Nowy Targ Plan ogólny gminy",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1211092",
        source_url="https://nowy-targ.geoportal-krajowy.pl/mpzp",
        label="Gmina Nowy Targ Geoportal MPZP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1211092",
        source_url="https://www.ugnowytarg.pl/assets/nowyTarg/media/files/c631ccc3-2fff-4b45-9bce-5bb9f6273fcd/zal-nr-1-tekst-zmiany-studium.pdf",
        label="Gmina Nowy Targ Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowy-wisnicz.geoportal-krajowy.pl/plan-ogolny",
        label="Nowy Wiśnicz Plan ogólny - Geoportal Krajowy",
        source_confidence=Decimal("0.88"),
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
        teryt_gmina="1216155",
        source_url="https://zabno.pl/wp-content/uploads/2024/11/PLAN-Ogolny.pdf",
        label="Żabno Plan ogólny PDF",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1215082",
        source_url="https://zawoja.geoportal-krajowy.pl/plan-ogolny",
        label="Zawoja Geoportal plan ogólny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1215082",
        source_url="https://ug.zawoja.pl/wp-content/uploads/2023/04/informacja-wersja-ostateczna.pdf",
        label="Zawoja informacja do studium PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1215082",
        source_url="https://www.ug.zawoja.pl/sites/zawoja.ug.pl/files/tekst_planu_projekt.pdf",
        label="Zawoja tekst planu projekt PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1215082",
        source_url="https://ug.zawoja.pl/wp-content/uploads/2023/09/2023.09.02_ZAWOJA-TEKST-zm-planu_do-wylozenia.pdf",
        label="Zawoja zmiana planu do wyłożenia PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1215082",
        source_url="https://ug.zawoja.pl/ogloszenie-wojta-gminy-zawoja-o-wylozeniu-do-publicznego-wgladu-projektu-zmiany-studium-uwarunkowan-i-kierunkow-zagospodarowania-przestrzennego-gminy-zawoja/",
        label="Zawoja wyłożenie projektu zmiany studium",
        source_confidence=Decimal("0.88"),
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
    HtmlIndexSignalSource(
        teryt_gmina="2408031",
        source_url="https://www.orzesze.pl/a%2C1695%2Cprzystapienie-do-sporzadzania-planu-ogolnego-zagospodarowania-przestrzennego-miasta-orzesze",
        label="Orzesze Plan ogólny - ogłoszenie",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2408031",
        source_url="https://morzesze.e-mapa.net/legislacja/mpzp/8647.html",
        label="Orzesze MPZP projekt 8647",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2414021",
        source_url="https://bip.imielin.pl/mfiles/2350/28/0/z/plan-og-lny_uchwa-a.pdf",
        label="Imielin Plan ogólny uchwała PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2414021",
        source_url="https://www.imielin.pl/files/fck/Studium_tresc.pdf",
        label="Imielin Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2417032",
        source_url="https://bip.gilowice.pl/9003",
        label="Gilowice Plan ogólny - BIP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2417032",
        source_url="https://bip.gilowice.pl/6111/dokument/17703",
        label="Gilowice MPZP dokument 17703",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2417032",
        source_url="https://bip.gilowice.pl/6111/dokument/4141",
        label="Gilowice Studium dokument 4141",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2417032",
        source_url="https://www.archiwum.gilowice.pl/miejscowy-plan-zagospodarowania-przestrzennego-dla-solectwa-gilowice-i-rychwald%2C2173%2Cakt.html",
        label="Gilowice MPZP sołectwa Gilowice i Rychwałd",
        source_confidence=Decimal("0.88"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2417032",
        source_url="https://www.archiwum.gilowice.pl/zdjecia/ak/zal/gilowice-uchwala-projekt_201807301535.pdf",
        label="Gilowice uchwała projekt PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://bip.gliwice.eu/planowanie-przestrzenne",
        label="Gliwice Planowanie przestrzenne - BIP",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://msip.gliwice.eu/portal-planistyczny-geoportal-planistyczny",
        label="Gliwice Geoportal planistyczny",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://msip.gliwice.eu/portal-planistyczny-mpzp-w-opracowaniu",
        label="Gliwice MPZP w opracowaniu",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://msip.gliwice.eu/portal-planistyczny-plan-ogolny-informacje-ogolne",
        label="Gliwice Plan ogólny - informacje ogólne",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://msip.gliwice.eu/add/file/1400005813.pdf",
        label="Gliwice Plan ogólny - materiał informacyjny PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://gliwice.eu/aktualnosci/miasto/rozpoczecie-nowej-procedury-planistycznej-osiedle-obroncow-pokoju",
        label="Gliwice Procedura planistyczna - os. Obrońców Pokoju",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://gliwice.eu/aktualnosci/miasto/rozpoczecie-nowej-procedury-planistycznej-rejon-ulicy-plazynskiego",
        label="Gliwice Procedura planistyczna - rejon ul. Płażyńskiego",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://gliwice.eu/aktualnosci/miasto/wylozenie-projektu-mpzp-dla-rejonu-ulic-piwnej-i-okopowej-od-16-sierpnia",
        label="Gliwice Wyłożenie projektu MPZP Piwna i Okopowa",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://bip.gliwice.eu/rada-miasta/projekty-uchwal/karta-projektu/14172",
        label="Gliwice Projekt uchwały 14172",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="2466011",
        source_url="https://geoportal.gliwice.eu/isdp/core/download/documents/.att/5-/CR8IZ9QJATKCMTRRFPHA/RUR_Na_Piasku_prezntacja_sesja_compressed.pdf",
        label="Gliwice Na Piasku prezentacja PDF",
        source_confidence=Decimal("0.92"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1262011",
        source_url="https://www.nowysacz.pl/content/resources/urzad/rada_miasta/prawo_lokalne/zal1_tekst_studium.pdf",
        label="Nowy Sącz Studium tekst PDF",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1262011",
        source_url="https://www.nowysacz.pl/content/resources/urzad/rada_miasta/porzadek_obrad/2023/VIII_SRMNS_93/p_xciii_1173_23_viii.pdf",
        label="Nowy Sącz Uchwała o planie ogólnym",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1262011",
        source_url="https://www.nowysacz.pl/prawo-lokalne/pl_zp",
        label="Nowy Sącz Prawo lokalne - planowanie przestrzenne",
        source_confidence=Decimal("0.90"),
    ),
    HtmlIndexSignalSource(
        teryt_gmina="1262011",
        source_url="https://www.nowysacz.pl/zagospodarowanie-przestrzenne/29103",
        label="Nowy Sącz MPZP projekt - konsultacje",
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
        response = await _fetch_source_response(source.source_url, headers=headers)
        source_type = _detect_source_type(source.source_url, response.headers.get("content-type"))
        if source_type == "pdf":
            page = _extract_pdf_text(response.content)
        else:
            page = response.text
            html_index_error = _detect_html_index_error(page)
            if html_index_error:
                raise ValueError(html_index_error)

        signals: list[PlanningSignal] = []
        if source_type == "gml":
            gml_signal = _parse_gml_signal(page, source)
            if gml_signal is not None:
                signals.append(gml_signal)

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
    province_prefix: Optional[str] = None,
) -> list[HtmlIndexSourceProbeResult]:
    results: list[HtmlIndexSourceProbeResult] = []
    for source in _HTML_INDEX_SIGNAL_REGISTRY:
        if teryt_gmina and source.teryt_gmina != teryt_gmina:
            continue
        if province_prefix and not source.teryt_gmina.startswith(province_prefix):
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
    if lowered_url.endswith(".gml") or "gml" in lowered_content_type or "xml" in lowered_content_type:
        return "gml"
    return "html_index"


def _extract_pdf_text(payload: bytes) -> str:
    reader = PdfReader(BytesIO(payload))
    chunks: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text:
            chunks.append(text)
    return "\n".join(chunks).strip()


def _parse_gml_signal(
    page: str,
    source: HtmlIndexSignalSource,
) -> PlanningSignal | None:
    xml_text = html.unescape(page or "")
    if not xml_text.strip():
        return None

    designation_normalized = normalize_designation_class("POG", xml_text)
    if designation_normalized not in POSITIVE_DESIGNATIONS:
        return None

    snippet = " ".join(re.sub(r"[<>=\"']", " ", xml_text).split())[:240]
    return PlanningSignal(
        teryt_gmina=source.teryt_gmina,
        signal_kind="planning_resolution",
        signal_status="formal_directional",
        designation_raw="POG",
        designation_normalized=designation_normalized,
        description=snippet,
        plan_name=f"POG {source.teryt_gmina}",
        uchwala_nr=None,
        effective_date=None,
        source_url=source.source_url,
        source_type="html_index",
        source_confidence=source.source_confidence,
        legal_weight=score_signal(
            signal_kind="planning_resolution",
            designation_normalized=designation_normalized,
            signal_status="formal_directional",
        ),
        geom=None,
        evidence_chain=[
            {
                "step": "html_index",
                "ref": source.source_url,
                "designation_raw": "POG",
                "title": snippet,
                "label": source.label,
            }
        ],
        updated_at=datetime.now(timezone.utc),
    )


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
    raw_page = html.unescape(page or "")

    if "mpzpregistry" in raw_page.lower():
        total_match = re.search(r'totalAmount"\s*:\s*\[0,(\d+)\]', raw_page, flags=re.IGNORECASE)
        title_matches = re.findall(r'name"\s*:\s*\[0,"([^"]+)"\]', raw_page, flags=re.IGNORECASE)
        total_amount = int(total_match.group(1)) if total_match else len(title_matches)
        if total_amount > 0 or title_matches:
            titles = [title.strip() for title in title_matches[:3] if title.strip()]
            summary = "; ".join(titles) if titles else "Rejestr obowiązujących MPZP"
            snippet = f"Geoportal registry lists {total_amount} obowiązujących MPZP: {summary}"[:240]
            designation_normalized = normalize_designation_class("MPZP", snippet)
            return PlanningSignal(
                teryt_gmina=source.teryt_gmina,
                signal_kind="planning_resolution",
                signal_status="formal_binding",
                designation_raw="MPZP",
                designation_normalized=designation_normalized,
                description=snippet,
                plan_name=f"MPZP registry {source.teryt_gmina}",
                uchwala_nr=None,
                effective_date=None,
                source_url=source.source_url,
                source_type=source_type,
                source_confidence=source.source_confidence,
                legal_weight=score_signal(
                    signal_kind="planning_resolution",
                    designation_normalized=designation_normalized,
                    signal_status="formal_binding",
                ),
                geom=None,
                evidence_chain=[
                    {
                        "step": "html_index",
                        "ref": source.source_url,
                        "designation_raw": "MPZP",
                        "title": snippet,
                        "label": source.label,
                        "registry_count": total_amount,
                    }
                ],
                updated_at=datetime.now(timezone.utc),
            )

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
