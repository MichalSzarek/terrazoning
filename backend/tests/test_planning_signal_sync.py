from app.services.planning_signal_sync import (
    HTML_INDEX_SIGNAL_REGISTRY,
    HtmlIndexSignalSource,
    _clean_html_fragment,
    _detect_source_type,
    _detect_html_index_error,
    _is_ssl_certificate_error,
    _parse_gml_signal,
    _parse_generic_planning_page_signal,
    _parse_studium_page_signal,
    _resolve_relative_href,
)


def test_clean_html_fragment_strips_tags_and_entities() -> None:
    raw = "<a href='gml.php?plan=000'>SUiKZP</a>&nbsp;<b>Zakopane</b>"
    assert _clean_html_fragment(raw) == "SUiKZP Zakopane"


def test_resolve_relative_href_joins_against_base_url() -> None:
    assert _resolve_relative_href(
        "https://mzakopane.e-mapa.net/wykazplanow/",
        "<a href='gml.php?plan=000'>APP</a>",
    ) == "https://mzakopane.e-mapa.net/wykazplanow/gml.php?plan=000"


def test_detect_html_index_error_flags_operator_error_pages() -> None:
    assert _detect_html_index_error("Błąd połączenia em_") is not None


def test_detect_source_type_handles_pdf_urls() -> None:
    assert (
        _detect_source_type(
            "https://rastry.gison.pl/mpzp-public/zarki/uchwaly/U_2016_112_XVII_studium_tekst.pdf",
            "application/pdf",
        )
        == "pdf"
    )


def test_detect_source_type_handles_gml_urls() -> None:
    assert (
        _detect_source_type(
            "https://bip.czerwionka-leszczyny.pl/pliki/1POG-Czerwionka-Leszczyny-30-03-2026,37000.gml",
            "application/gml+xml",
        )
        == "gml"
    )


def test_is_ssl_certificate_error_detects_verify_failure() -> None:
    assert _is_ssl_certificate_error(Exception("certificate verify failed"))
    assert _is_ssl_certificate_error(Exception("SSL: CERTIFICATE_VERIFY_FAILED"))
    assert not _is_ssl_certificate_error(Exception("404 Not Found"))


def test_parse_generic_planning_page_signal_detects_plan_ogolny_article() -> None:
    page = (
        "<html><body><h1>Plan ogólny gminy</h1>"
        "<p>Obecnie trwa sporządzenie projektu planu.</p>"
        "<p>Dnia 27 czerwca 2024 r. Rada Miasta Zakopane podjęła uchwałę w sprawie "
        "przystąpienia do sporządzenia planu ogólnego miasta Zakopane.</p>"
        "</body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1217011",
        source_url="https://www.zakopane.pl/zagospodarowanie-przestrzenne/plan-ogolny-gminy/",
        label="Zakopane Plan ogólny gminy",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.signal_status == "formal_preparatory"
    assert signal.plan_name == "POG 1217011"
    assert signal.source_url == source.source_url
    assert signal.designation_normalized == "unknown"
    assert signal.legal_weight == 0


def test_html_index_registry_includes_plan_ogolny_sources() -> None:
    urls = {source.source_url for source in HTML_INDEX_SIGNAL_REGISTRY}
    assert (
        "https://www.knurow.pl/miasto-knurow/ogloszenia-urzedu/OBWIESZCZENIE-PREZYDENTA-MIASTA-KNUROW-z-dnia-7-marca-2024-r/idn:4390"
        in urls
    )
    assert "https://www.knurow.pl/download/Uzasadnienie,812.pdf" in urls
    assert "https://www.knurow.pl/miasto-knurow/ogloszenia-urzedu/Obwieszczenie-Prezydenta-Miasta-Knurow/idn:5841" in urls
    assert "https://www.knurow.pl/download/Uchwala-XV_165_2025-RM-Knurow,1182.pdf" in urls
    assert "https://www.katowice.eu/plan-ogolny" in urls
    assert "https://bip.katowice.eu/Lists/Dokumenty/Attachments/150551/Uzasadnienie%20do%20projektu%20POG%20Katowice%20-%20Etap%20opiniowania%20i%20uzgodnie%C5%84.pdf" in urls
    assert "https://bip.katowice.eu/PublishingImages/Planowanie%20Przestrzenne/tekst%20Studium%20cz%C4%99%C5%9B%C4%87%201%20-%20Uwarunkowania%20zagospodarowania%20przestrzennego.pdf" in urls
    assert "https://bip.czerwionka-leszczyny.pl/pliki/Uzasadnienie-POG-CZERWIONKA-MARZEC-2026,36998.pdf" in urls
    assert "https://bip.czerwionka-leszczyny.pl/pliki/1POG-Czerwionka-Leszczyny-30-03-2026,37000.gml" in urls
    assert "https://www.zakopane.pl/zagospodarowanie-przestrzenne/plan-ogolny-gminy/" in urls
    assert (
        "https://gminaoswiecim.pl/pl/1960/24294/sporzadzenie-planu-ogolnego-gminy-oswiecim-ogloszenie-wojta-gminy-oswiecim.html"
        in urls
    )
    assert "https://nowywisnicz.pl/aktualnosci/planowanie-przestrzenne/" in urls
    assert "https://nowywisnicz.e-mapa.net/implementation/nowywisnicz/pln/pelna_tresc/000.pdf" in urls
    assert "https://nowy-wisnicz.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://www.brzesko.pl/wpis/87444%2Copracowanie-planu-ogolnego-zagospodarowania-przestrzennego-dla-gminy-brzesko" in urls
    assert "https://brzesko.geoportal-krajowy.pl/mpzp" in urls
    assert "https://www.brzesko.pl/artykul/222%2Cplanowanie-przestrzenne" in urls
    assert "https://jaslo.e-mapa.net/wykazplanow/" in urls
    assert "https://rejestrplanowogolnych.pl/?teryt=180504_2" in urls
    assert "https://rastry.gison.pl/mpzp-public/jaslogmina/uchwaly/U_2018_433_LXVIII_studium.pdf" in urls
    assert "https://uglezajsk.bip.gov.pl/mpzp-giedlarowa/o-b-w-i-e-s-z-c-z-e-n-i-e-wojta-gminy-lezajsk-z-dnia-27-02-2024-o-przystapieniu-do-sporzadzania-miejscowego-planu-zagospodarowania-przestrzennego-terenu-wsi-giedlarowa.html" in urls
    assert "https://uglezajsk.bip.gov.pl/planowanie-przestrzenne/zbiory-danych-przestrzennych/zbior-app-dla-mpz/zbior-danych-mpzp-makemaker.html" in urls
    assert "https://uglezajsk.bip.gov.pl/planowanie-przestrzenne/zbiory-danych-przestrzennych/zbior-app-dla-studium/zbior-danych-app-dla-studium-makemaker.html" in urls
    assert "https://bip.boguchwala.pl/pl/404-menu-tematyczne/12304-planowanie-przestrzenne.html" in urls
    assert "https://boguchwala.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://rastry.gison.pl/mpzp-public/boguchwala/uchwaly/U_2020_354_XXIX_studium_tekst.pdf" in urls
    assert "https://rastry.gison.pl/mpzp-public/boguchwala/uchwaly/U_2019_218_XV.pdf" in urls
    assert "https://www.ugradymno.pl/asp/plan-ogolny-informacje%2C1%2Cartykul%2C1%2C1625" in urls
    assert "https://www.ugradymno.pl/asp/core/pdf.asp?akcja=artykul&artykul=1551&menu=1" in urls
    assert "https://radymno.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://radymno.geoportal-krajowy.pl/mpzp" in urls
    assert "https://bip.tarnowiec.eu/planowanie-przestrzenne/238" in urls
    assert "https://bip.tarnowiec.eu/projekty-mpzp/290" in urls
    assert "https://tarnowiec.eu/aktualnosc-4123-przystapienie_do_sporzadzenia_planu.html" in urls
    assert "https://gminadebica.e-mapa.net/wykazplanow/" in urls
    assert "https://jodlowa.e-mapa.net/wykazplanow/" in urls
    assert "https://dukla.e-mapa.net/wykazplanow/" in urls
    assert "https://dukla.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://www.dukla.pl/pl/dla-mieszkancow/mapy-i-plany-79/wnioski-do-planu-ogolnego-226" in urls
    assert "https://www.dukla.pl/files/_source/2025/01/ogloszenie%20na%20BIP%20i%20na%20strone%20gminy.pdf" in urls
    assert "https://narol.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://narol.geoportal-krajowy.pl/mpzp" in urls
    assert "https://debica.geoportal-krajowy.pl/" in urls
    assert "https://debica.geoportal-krajowy.pl/mpzp" in urls
    assert "https://jodlowa.geoportal-krajowy.pl/" in urls
    assert "https://jodlowa.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://pilzno.geoportal-krajowy.pl/" in urls
    assert "https://pilzno.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://pilzno.geoportal-krajowy.pl/mpzp" in urls
    assert "https://czermin-mielecki.geoportal-krajowy.pl/" in urls
    assert "https://czermin-mielecki.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://glogow-malopolski.geoportal-krajowy.pl/" in urls
    assert "https://glogow-malopolski.geoportal-krajowy.pl/mpzp" in urls
    assert "https://iwierzyce.e-mapa.net/wykazplanow/" in urls
    assert "https://niebylec.geoportal-krajowy.pl/" in urls
    assert "https://niebylec.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://strzyzow.geoportal-krajowy.pl/" in urls
    assert "https://strzyzow.geoportal-krajowy.pl/mpzp" in urls
    assert "https://grebow.geoportal-krajowy.pl/" in urls
    assert "https://grebow.geoportal-krajowy.pl/mpzp" in urls
    assert "https://www.wielka-wies.pl/o-gminie/aktualnosci/ogloszenie-plan-ogolny/" in urls
    assert "https://wielka-wies.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://old.wielka-wies.pl/media/191132/zal-1-wielka-wies-studium-tekst-ujednolicony.pdf" in urls
    assert "https://www.chrzanow.pl/gmina/planowanie-przestrzenne/plan-ogolny" in urls
    assert "https://chrzanow.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://www.chrzanow.pl/gmina/planowanie-przestrzenne/plany-zagospodarowania---projekty" in urls
    assert "https://www.chrzanow.pl/aktualnosci/plan-ogolny-gminy-chrzanow--mozna-skladac-wnioski%2C2737" in urls
    assert "https://www.chrzanow.pl/gmina/planowanie-przestrzenne/plany-zagospodarowania---projekty/projekt-zmiany-mpzp-dla-terenu-gorniczego-babice-i" in urls
    assert "https://www.chrzanow.pl/storage/file/core_files/2024/3/18/52f16e1503ab2ccac845e1e91f12e2aa/Protok%C3%B3%C5%82%20z%20dyskusji%20publicznej_2024.pdf" in urls
    assert "https://www.sekowa.pl/strefa_mieszkanca/ogloszenie-o-zamieszczeniu-danych-o-projekcie-zmiany-miejscowego-planu-zagospodarowania-przestrzennego-gminy-sekowa/" in urls
    assert "https://www.sekowa.pl/strefa_mieszkanca/ogloszenie-wojta-gminy-sekowa-z-dnia-30-stycznia-2025-r-o-przystapieniu-do-sporzadzenia-zmiany-miejscowego-planu-zagospodarowania-przestrzennego-gminy-sekowa/" in urls
    assert "https://www.sekowa.pl/plan-zagospodarowania-przestrzennego/" in urls
    assert "https://rastry.gison.pl/mpzp-public/korzenna/uchwaly/U_2018_375_XXXIV_studium_tekst.pdf" in urls
    assert "https://www.korzenna.pl/plan-ogolny-zamiast-studium-uwarunkowan-wnioski-do-27-grudnia/" in urls
    assert "https://www.korzenna.pl/setki-wnioskow-do-planu-gminy/" in urls
    assert "https://www.korzenna.pl/blog/2024/12/13/informacja-o-nieobowiazywaniu-zapisow-studium-w-planie-ogolnym-gminy-korzenna/" in urls
    assert "https://igolomia-wawrzenczyce.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://www.iwanowice.pl/dla-mieszkanca/plan-ogolny-gminy-iwanowice/" in urls
    assert "https://www.iwanowice.pl/zapuveer/2024/09/SUiKZP_Iwanowice_Zalacznik_2_Ustalenia_2024-09.pdf.pdf" in urls
    assert "https://www.iwanowice.pl/zapuveer/2024/09/SUiKZP_Iwanowice_Zalacznik_1_Uwarunkowania_2024-09.pdf.pdf" in urls
    assert "https://iwanowice.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://www.iwanowice.pl/ogloszenie-wojta-iwanowice-projekt-zmiany-studium/" in urls
    assert "https://iwanowice.pl/wp-content/uploads/2021/11/MPZP_TEKS.pdf" in urls
    assert "https://skala.pl/studium/" in urls
    assert "https://skala.pl/wp-content/uploads/2024/01/13_KIERUNKI_SKALA_wylozenie.pdf" in urls
    assert "https://skala.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://skala.pl/dokumenty/plan-zagospodarowania/" in urls
    assert "https://skala.pl/obwieszczenie-burmistrza-miasta-i-gminy-skala-o-wylozeniu-do-publicznego-wgladu-projektu-studium-uwarunkowan-i-kierunkow-zagospodarowania-przestrzennego-21/" in urls
    assert "https://www.gminaskawina.pl/mieszkancy/informacje-praktyczne/miejscowy-plan-zagospodarowania-przestrzennego/studium-uwarunkowan-i-kierunkow-zagospodarowania-przestrzennego-gminy-skawina" in urls
    assert "https://rastry.gison.pl/mpzp-public/skawina_wylozenie/uchwaly/studium_wylozenie_kierunki.pdf" in urls
    assert "https://skawina.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://www.gminaskawina.pl/assets/skawina/media/files/8613e3e7-8b67-4c9f-87de-765280dc4049/uchwala-nr-ii-16-24-rady-miejskiej-w-skawinie.pdf" in urls
    assert "https://www.gminaskawina.pl/mieszkancy/informacje-praktyczne/miejscowy-plan-zagospodarowania-przestrzennego/aktualnosci-gp/2025/ogloszenie-burmistrza-miasta-i-gminy-skawina-za-dnia-13-czerwca-2025-r" in urls
    assert "https://www.gminaskawina.pl/assets/skawina/media/files/f9ec549d-02d6-4836-9b1a-cef2d2e9de25/projekt-zmiany-mpzp-miasta-skawina-kdd-wylozenie-20-11-2023.pdf" in urls
    assert "https://zabierzow.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://zabierzow.org.pl/572-plan-ogolny.html" in urls
    assert "https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/plan-ogolny-gminy-wadowice/" in urls
    assert "https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/system-informacji-przestrzennej/" in urls
    assert "https://zabno.pl/wp-content/uploads/2024/11/PLAN-Ogolny.pdf" in urls
    assert "https://zawoja.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://ug.zawoja.pl/wp-content/uploads/2023/04/informacja-wersja-ostateczna.pdf" in urls
    assert "https://www.ug.zawoja.pl/sites/zawoja.ug.pl/files/tekst_planu_projekt.pdf" in urls
    assert "https://ug.zawoja.pl/wp-content/uploads/2023/09/2023.09.02_ZAWOJA-TEKST-zm-planu_do-wylozenia.pdf" in urls
    assert "https://ug.zawoja.pl/ogloszenie-wojta-gminy-zawoja-o-wylozeniu-do-publicznego-wgladu-projektu-zmiany-studium-uwarunkowan-i-kierunkow-zagospodarowania-przestrzennego-gminy-zawoja/" in urls
    assert "https://bip.kamienicapolska.pl/artykul/plan-ogolny" in urls
    assert "https://www.kruszyna.pl/plan-ogolny-gminy-kruszyna/" in urls
    assert "https://www.kozieglowy.pl/aktualnosci/4985" in urls
    assert "https://www.ksiazwielki.eu/index.php/dla-mieszkanca/ogloszenia-i-komunikaty/528-ogloszenie-o-rozpoczeciu-konsultacji-spolecznych-projektu-planu-ogolnego-miasta-i-gminy-ksiaz-wielki" in urls
    assert "https://e-mapa.net/plan_ogolny/120804-ksiaz-wielki" in urls
    assert "https://www.ugnowytarg.pl/strefy/planowanie-przestrzenne-i-budownictwo/plan-ogolny-gminy-nowy-targ" in urls
    assert "https://nowy-targ.geoportal-krajowy.pl/mpzp" in urls
    assert "https://www.ugnowytarg.pl/assets/nowyTarg/media/files/c631ccc3-2fff-4b45-9bce-5bb9f6273fcd/zal-nr-1-tekst-zmiany-studium.pdf" in urls
    assert "https://www.orzesze.pl/a%2C1695%2Cprzystapienie-do-sporzadzania-planu-ogolnego-zagospodarowania-przestrzennego-miasta-orzesze" in urls
    assert "https://morzesze.e-mapa.net/legislacja/mpzp/8647.html" in urls
    assert "https://www.bip.wreczyca-wielka.akcessnet.net/index.php?a=0&id=587&idg=3&x=65&y=10" in urls
    assert "https://wreczyca-wielka.pl/aktualnosc-1167-ogloszenie.html" in urls
    assert "https://www.wreczyca-wielka.pl/aktualnosc-281-obwieszczenie_o_przystapieniu_do.html" in urls
    assert "https://www.bip.wreczyca-wielka.akcessnet.net/upload/20180119081806odmqcs0uipum.pdf" in urls
    assert "https://www.bip.wreczyca-wielka.akcessnet.net/upload/20170607123606h7nz89qvgekf.pdf" in urls
    assert "https://www.bip.wreczyca-wielka.akcessnet.net/upload/plik%2C20250828213836%2Cuzasadnienie_do_planu_ogolnego_gminy_wreczyca_wielka.pdf" in urls
    assert "https://www.imielin.pl/pl/205/7551/plan-ogolny-miasta.html" in urls
    assert "https://bip.imielin.pl/pl/2350/0/plan-ogolny.html" in urls
    assert "https://bip.imielin.pl/mfiles/2369/28/0/z/uzasadnienie-do-planu-og-lnego.pdf" in urls
    assert "https://bip.imielin.pl/mfiles/2350/28/0/z/plan-og-lny_uchwa-a.pdf" in urls
    assert "https://www.imielin.pl/files/fck/Studium_tresc.pdf" in urls
    assert "https://szczekociny.geoportal-krajowy.pl/plan-ogolny" in urls
    assert "https://mapa.inspire-hub.pl/upload/141_XXI_2016_SUiKZP_tekst__szczekociny.pdf?action_type=3" in urls
    assert "https://bip.szczekociny.pl/res/serwisy/pliki/13447905?version=1.0" in urls
    assert "https://bip.szczekociny.pl/res/serwisy/pliki/13447918?version=1.0" in urls
    assert "https://bip.szczekociny.pl/res/serwisy/pliki/42216826?version=1.0" in urls
    assert "https://bip.szczekociny.pl/res/serwisy/pliki/42216838?version=1.0" in urls
    assert "https://bip.czerwionka-leszczyny.pl/informacje_urzedu/plan-ogolny-gminy-i-miasta-czerwionka-leszczyny-pog" in urls
    assert "https://bip.bojszowy.pl/pl/3144/0/plan-ogolny.html" in urls
    assert "https://bip.bojszowy.pl/pl/3145/26261/projekt-planu-ogolnego-etap-opiniowania-i-uzgadniania.html" in urls
    assert "https://bip.gilowice.pl/9003" in urls
    assert "https://bip.gilowice.pl/6111/dokument/17703" in urls
    assert "https://bip.gilowice.pl/6111/dokument/4141" in urls
    assert "https://www.archiwum.gilowice.pl/miejscowy-plan-zagospodarowania-przestrzennego-dla-solectwa-gilowice-i-rychwald%2C2173%2Cakt.html" in urls
    assert "https://www.archiwum.gilowice.pl/zdjecia/ak/zal/gilowice-uchwala-projekt_201807301535.pdf" in urls
    assert "https://bip.gliwice.eu/planowanie-przestrzenne" in urls
    assert "https://msip.gliwice.eu/portal-planistyczny-geoportal-planistyczny" in urls
    assert "https://msip.gliwice.eu/portal-planistyczny-mpzp-w-opracowaniu" in urls
    assert "https://msip.gliwice.eu/portal-planistyczny-plan-ogolny-informacje-ogolne" in urls
    assert "https://msip.gliwice.eu/add/file/1400005813.pdf" in urls
    assert "https://gliwice.eu/aktualnosci/miasto/rozpoczecie-nowej-procedury-planistycznej-osiedle-obroncow-pokoju" in urls
    assert "https://gliwice.eu/aktualnosci/miasto/rozpoczecie-nowej-procedury-planistycznej-rejon-ulicy-plazynskiego" in urls
    assert "https://gliwice.eu/aktualnosci/miasto/wylozenie-projektu-mpzp-dla-rejonu-ulic-piwnej-i-okopowej-od-16-sierpnia" in urls
    assert "https://bip.gliwice.eu/rada-miasta/projekty-uchwal/karta-projektu/14172" in urls
    assert "https://geoportal.gliwice.eu/isdp/core/download/documents/.att/5-/CR8IZ9QJATKCMTRRFPHA/RUR_Na_Piasku_prezntacja_sesja_compressed.pdf" in urls
    assert "https://www.nowysacz.pl/content/resources/urzad/rada_miasta/prawo_lokalne/zal1_tekst_studium.pdf" in urls
    assert "https://www.nowysacz.pl/content/resources/urzad/rada_miasta/porzadek_obrad/2023/VIII_SRMNS_93/p_xciii_1173_23_viii.pdf" in urls
    assert "https://www.nowysacz.pl/prawo-lokalne/pl_zp" in urls
    assert "https://www.nowysacz.pl/zagospodarowanie-przestrzenne/29103" in urls
    assert "https://bip.kamienicapolska.pl/artykul/studium-uwarunkowan" in urls
    assert "https://bip.kamienicapolska.pl/artykul/opiniowanie-z-gminna-komisja-urbanistyczno-architektoniczna" in urls
    assert "https://www.zarki.bip.jur.pl/artykuly/6686" in urls
    assert "https://www.zarki.bip.jur.pl/kategorie/projekt_zmiany_studium_uwarunkowan_i_kierunkow" in urls
    assert "https://rastry.gison.pl/mpzp-public/zarki/uchwaly/U_2016_112_XVII_studium_tekst.pdf" in urls


def test_parse_generic_planning_page_signal_detects_wnioski_do_planu_ogolnego() -> None:
    page = (
        "<html><body><article>"
        "<h1>WAŻNE !!! Jeszcze do piątku można składać wnioski do planu ogólnego Gminy Nowy Wiśnicz</h1>"
        "<p>Burmistrz Nowego Wiśnicza przypomina, że do dnia 12.07.2024r. można składać wnioski do planu ogólnego.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1201065",
        source_url="https://nowywisnicz.pl/aktualnosci/planowanie-przestrzenne/",
        label="Nowy Wiśnicz Planowanie przestrzenne",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.signal_status == "formal_preparatory"
    assert signal.plan_name == "POG 1201065"
    assert signal.source_url == source.source_url
    assert signal.designation_normalized == "unknown"
    assert signal.legal_weight == 0


def test_parse_studium_page_signal_detects_current_studium_page() -> None:
    page = (
        "<html><body><section>"
        "<h1>Obowiązujące studium uwarunkowań i kierunków zagospodarowania przestrzennego gminy Wadowice</h1>"
        "<p>System Informacji Przestrzennej obejmuje m.in. miejscowe plany zagospodarowania przestrzennego i studium.</p>"
        "</section></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1218095",
        source_url="https://wadowice.pl/urzad/wydzialy/wydzial-planowania-przestrzennego/system-informacji-przestrzennej/",
        label="Wadowice Studium uwarunkowań",
    )
    signal = _parse_studium_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.signal_status == "formal_directional"
    assert signal.plan_name == "SUiKZP 1218095"
    assert signal.designation_raw == "SUiKZP"
    assert signal.source_url == source.source_url
    assert signal.designation_normalized == "unknown"
    assert signal.legal_weight == 0


def test_parse_generic_planning_page_signal_detects_bip_preparatory_phrases() -> None:
    page = (
        "<html><body><article>"
        "<h1>Plan ogólny gminy Kamienica Polska</h1>"
        "<p>Etap opiniowania i uzgadniania. Opiniowanie z Gminną Komisją Urbanistyczno-Architektoniczną.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2404042",
        source_url="https://bip.kamienicapolska.pl/artykul/plan-ogolny",
        label="Kamienica Polska Plan ogólny - BIP",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_status == "formal_preparatory"
    assert signal.source_url == source.source_url
    assert signal.designation_normalized == "unknown"
    assert signal.legal_weight == 0


def test_parse_generic_planning_page_signal_classifies_mpzp_project() -> None:
    page = (
        "<html><body><article>"
        "<h1>Przystąpienie do sporządzenia miejscowego planu zagospodarowania przestrzennego</h1>"
        "<p>Trwa sporządzenie projektu miejscowego planu zagospodarowania przestrzennego dla wybranego obszaru.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2414042",
        source_url="https://example.org/mpzp-project",
        label="Bojszowy MPZP project",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "mpzp_project"
    assert signal.signal_status == "formal_preparatory"
    assert signal.designation_raw == "MPZP"
    assert signal.plan_name == "MPZP 2414042"
    assert signal.designation_normalized == "unknown"
    assert signal.legal_weight == 0


def test_parse_generic_planning_page_signal_detects_geoportal_krajowy_mpzp_registry() -> None:
    page = (
        '{"mpzpData":[0,{"totalAmount":[0,14],"mpzpRegistry":[1,['
        '[0,{"name":[0,"Uchwała nr XLVI/17/2022 Rady Gminy Radymno z dnia 24 lutego 2022 r. '
        'w sprawie uchwalenia miejscowego planu zagospodarowania przestrzennego Sośnica 2"]}],'
        '[0,{"name":[0,"Uchwała nr XXXI/17/2021 Rady Gminy Radymno w sprawie uchwalenia '
        'miejscowego planu zagospodarowania przestrzennego Zaleska Wola I"]}]'
        ']}]}'
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1804082",
        source_url="https://radymno.geoportal-krajowy.pl/mpzp",
        label="Radymno MPZP registry",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.signal_status == "formal_binding"
    assert signal.designation_raw == "MPZP"
    assert signal.plan_name == "MPZP registry 1804082"
    assert signal.source_url == source.source_url
    assert signal.evidence_chain[0]["registry_count"] == 14
    assert "Sośnica 2" in (signal.description or "")


def test_parse_generic_planning_page_signal_reads_positive_symbols_from_pdf_text() -> None:
    page = (
        "UZASADNIENIE Przedmiotem niniejszego projektu uchwały jest przystąpienie do sporządzenia zmiany "
        "miejscowego planu zagospodarowania przestrzennego miasta Knurów. "
        "Obsługa komunikacyjna terenów objętych planem, tj. Z14.1UP-UC, Z14.2UP-UC, Z14.UP, Z14.UC oraz Z14.ZD, "
        "może odbywać się za pośrednictwem dróg publicznych."
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2405011",
        source_url="https://www.knurow.pl/download/Uzasadnienie,812.pdf",
        label="Knurów Uzasadnienie zmiany MPZP Szpitalna i 26 Stycznia",
    )
    signal = _parse_generic_planning_page_signal(page, source, source_type="pdf")
    assert signal is not None
    assert signal.signal_kind == "mpzp_project"
    assert signal.designation_normalized == "service"
    assert signal.legal_weight > 0


def test_parse_gml_signal_reads_positive_designation() -> None:
    page = (
        '<app:profilPodstawowy xlink:title="strefa wielofunkcyjna z zabudową mieszkaniową jednorodzinną" />'
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2412014",
        source_url="https://bip.czerwionka-leszczyny.pl/pliki/1POG-Czerwionka-Leszczyny-30-03-2026,37000.gml",
        label="Czerwionka-Leszczyny POG - GML projekt",
    )
    signal = _parse_gml_signal(page, source)
    assert signal is not None
    assert signal.designation_normalized == "residential"
    assert signal.legal_weight > 0


def test_parse_generic_planning_page_signal_classifies_studium_change_context() -> None:
    page = (
        "<html><body><article>"
        "<h1>Zmiana studium uwarunkowań i kierunków zagospodarowania przestrzennego gminy</h1>"
        "<p>Trwa sporządzenie projektu dokumentu oraz etap uzgadniania.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="1218095",
        source_url="https://example.org/studium-change",
        label="Wadowice Studium change",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_kind == "planning_resolution"
    assert signal.designation_raw == "SUiKZP"
    assert signal.plan_name == "SUiKZP 1218095"
    assert signal.signal_status == "formal_preparatory"


def test_parse_generic_planning_page_signal_detects_zbieranie_uwag_and_rejestr_phrase() -> None:
    page = (
        "<html><body><article>"
        "<h1>Plan ogólny Miasta Imielin</h1>"
        "<p>Zakończyliśmy etap zbierania uwag do Planu Ogólnego.</p>"
        "<p>Informacje publikujemy w rejestrze planów ogólnych.</p>"
        "</article></body></html>"
    )
    source = HtmlIndexSignalSource(
        teryt_gmina="2414021",
        source_url="https://www.imielin.pl/pl/205/7551/plan-ogolny-miasta.html",
        label="Imielin Plan Ogólny Miasta - Aktualności",
    )
    signal = _parse_generic_planning_page_signal(page, source)
    assert signal is not None
    assert signal.signal_status == "formal_preparatory"
    assert signal.source_url == source.source_url
    assert signal.designation_normalized == "unknown"
    assert signal.legal_weight == 0
