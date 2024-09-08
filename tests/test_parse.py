import pytest
from datetime import datetime, timedelta, tzinfo
import os
from nflogic.parse import (
    valid_int,
    valid_float,
    DictParser,
)


SCRIPT_DIR = os.path.split(os.path.realpath(__file__))[0]
TEST_XML = os.path.join(SCRIPT_DIR, "test_xml.xml")


class tzBrazilEast(tzinfo):
    def utcoffset(self, dt: datetime | None = None) -> timedelta:
        return timedelta(hours=-3)

    def dst(self, dt: datetime | None = None):
        return None


@pytest.mark.parametrize(
    "val,expected",
    [("ABc", False), ("584", True), ("8.9", False), (12345, True), (221.9, True)],
)
def test_valid_int(val, expected):
    assert valid_int(val) == expected


@pytest.mark.parametrize(
    "val,expected",
    [("ABc", False), ("584", True), ("8.9", True), (12345, True), (221.9, True)],
)
def test_valid_float(val, expected):
    assert valid_float(val) == expected


def test_get_data():
    dp = DictParser(path=TEST_XML)
    dp.parse()

    assert dp.data["ChaveNFe"] == "26240811122233344455550010029490131741852894"
    assert dp.data["DataHoraEmi"] == datetime(
        2024, 8, 31, 16, 17, 16, tzinfo=tzBrazilEast()
    )
    assert dp.data["TotalProdutos"] == "996.85"
    assert dp.data["TotalDesconto"] == "0.00"
    assert dp.data["TotalTributos"] == "348.77"
