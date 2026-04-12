from decimal import Decimal

from scraper.extractors.price import extract_price_from_text, parse_price_value


def test_parse_price_value_handles_polish_commas_and_nbsp() -> None:
    assert parse_price_value("129 000,00 zł") == Decimal("129000.00")
    assert parse_price_value("129\u00A0000,00 zł") == Decimal("129000.00")
    assert parse_price_value("129000 zł") == Decimal("129000")


def test_parse_price_value_handles_dots_and_pln() -> None:
    assert parse_price_value("1.234.567,89 PLN") == Decimal("1234567.89")


def test_extract_price_from_text_finds_first_pln_amount() -> None:
    text = "Cena wywoławcza wynosi 853 333,33 zł, wadium 20 000 zł."
    assert extract_price_from_text(text) == Decimal("853333.33")


def test_extract_price_from_text_prefers_cena_wywolawcza_over_postapienie() -> None:
    text = "Cena wywołania 9 789,16 zł. Najniższe postąpienie 98,00 zł."
    assert extract_price_from_text(text) == Decimal("9789.16")


def test_extract_price_from_text_ignores_postapienie_when_multiple_pln_values_exist() -> None:
    text = "Cena wywołania 10 896,43 zł, najniższe postąpienie 109,00 zł."
    assert extract_price_from_text(text) == Decimal("10896.43")


def test_extract_price_from_text_prefers_oszacowanie_context() -> None:
    text = "Wartość oszacowania nieruchomości wynosi 129 000,00 zł, rękojmia 12 900,00 zł."
    assert extract_price_from_text(text) == Decimal("129000.00")


def test_extract_price_from_text_skips_wadium_only_amount() -> None:
    text = "Wadium 20 000 zł. Najniższe postąpienie 2 000 zł."
    assert extract_price_from_text(text) is None


def test_extract_price_from_text_handles_sale_price_formula() -> None:
    text = "Nieruchomość może być sprzedana za cenę nie niższą niż 44 250,00 zł, rękojmia 4 425,00 zł."
    assert extract_price_from_text(text) == Decimal("44250.00")


def test_extract_price_from_text_handles_three_quarters_of_appraisal() -> None:
    text = "Cena wywołania stanowiąca trzy czwarte sumy oszacowania wynosi 78 500,00 zł."
    assert extract_price_from_text(text) == Decimal("78500.00")


def test_extract_price_from_text_handles_kwote_phrase() -> None:
    text = "Cena wywoławcza określona na kwotę 129 000,00 zł, rękojmia 12 900,00 zł."
    assert extract_price_from_text(text) == Decimal("129000.00")


def test_extract_price_from_text_ignores_rekojmia_w_kwocie() -> None:
    text = "Rękojmia w kwocie 20 000,00 zł. Cena wywołania wynosi kwotę 180 000,00 zł."
    assert extract_price_from_text(text) == Decimal("180000.00")
