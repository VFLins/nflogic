from typing import TypedDict, get_type_hints
from datetime import datetime
from collections import OrderedDict
from pathlib import Path
import inspect
import xmltodict
import os
import re


SCRIPT_PATH = os.path.realpath(__file__)
BINDIR = os.path.join(os.path.split(SCRIPT_PATH)[0], "bin")
__funcname__ = lambda: inspect.stack()[1][3]


# TYPES
###############

ParserInput = TypedDict("ParserInput", {"path": str, "buy": bool})
PayInfo = TypedDict("PayInfo", {"type": str, "amount": str})
TotalInfo = TypedDict(
    "TotalInfo", {"products": float, "discount": float, "taxes": float}
)
FactParserData = TypedDict(
    "FactParserData",
    {
        "ChaveNFe": str,
        "DataHoraEmi": datetime,
        "PagamentoTipo": str,
        "PagamentoValor": str,
        "TotalProdutos": float,
        "TotalDesconto": float,
        "TotalTributos": float,
    },
)


class KeyType(str):
    def __init__(self) -> None:
        super().__init__()


class ListOfNumbersType(str):
    def __init__(self) -> None:
        super().__init__()


class FloatCoercibleType(str):
    def __init__(self) -> None:
        super().__init__()


class ParserInitError(Exception):
    """Error class that signals an error encountered in"""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ParserParseError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ParserValidationError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


# DATA VALIDATION
###############


def convert_to_list_of_numbers(inp: list[str]) -> ListOfNumbersType:
    list_of_num = [float(i) for i in inp]
    return str(list_of_num).replace(",", ";").replace(" ", "")


def convert_from_list_of_numbers(inp: ListOfNumbersType) -> list[float]:
    nums_list = inp.replace("[", "").replace("]", "").split(";")
    return [float(i) for i in nums_list]


def valid_int(val: any) -> bool:
    """Return `True` if `val` is of type `int` or coercible, `False` otherwise."""
    try:
        _ = int(val)
        return True
    except ValueError:
        return False


def valid_float(val: any) -> bool:
    """Return `True` if `val` is of type `float` or coercible, `False` otherwise."""
    try:
        _ = float(val)
        return True
    except ValueError:
        return False


def valid_list_of_numbers(val: str) -> bool:
    """Return `True` if the string in `val` can be converted to a list of numbers separated by semicolon, `False` otherwise."""
    if valid_float(val):
        return valid_float(val)
    return bool(re.match(r"^\[(?:[0-9.;])*\]$", val))


def valid_key(val) -> bool:
    if (type(val) == str) and (len(val) == 44) and val.isdigit():
        return True
    return False


class RowElem:
    """
    Generic class for validating parsed data. It's children must:
    1. Have annotated variable names with the corresponding data type
    2. Run `super().__init__(**kwargs)` where kwargs are the parameters specified in `self.__init__()`
    """

    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            self.__setattr__(name, value)
        self._validate_and_assign()

    def _validate_and_assign(self):
        types = self.__init__.__annotations__
        self.values = []

        for var in types.keys():
            value = getattr(self, var)

            if types[var] == KeyType:
                if not valid_key(value):
                    raise ValueError(f"Invalid value {var}: {value}")
                self.values.append(value)
                continue

            if types[var] == datetime:
                if type(value) != datetime:
                    raise ValueError(f"Invalid value in {var}: {value}")
                self.values.append(value.strftime("%Y-%m-%d %H:%M:%S %z"))
                continue

            if types[var] == ListOfNumbersType:
                if not valid_list_of_numbers(value):
                    raise ValueError(f"Invalid value in {var}: {value}")
                self.values.append(value)
                continue

            if types[var] == FloatCoercibleType:
                if not valid_float(value):
                    raise ValueError(f"Invalid value in {var}: {value}")
                self.values.append(float(value))
                continue

        self.values = tuple(self.values)


class FactRowElem(RowElem):
    def __init__(
        self,
        ChaveNFe: KeyType,
        DataHoraEmi: datetime,
        PagamentoTipo: ListOfNumbersType,
        PagamentoValor: ListOfNumbersType,
        TotalProdutos: FloatCoercibleType,
        TotalDesconto: FloatCoercibleType,
        TotalTributos: FloatCoercibleType,
    ):
        super().__init__(
            **{
                "ChaveNFe": ChaveNFe,
                "DataHoraEmi": DataHoraEmi,
                "PagamentoTipo": PagamentoTipo,
                "PagamentoValor": PagamentoValor,
                "TotalProdutos": TotalProdutos,
                "TotalDesconto": TotalDesconto,
                "TotalTributos": TotalTributos,
            }
        )


# PARSER
###############


class BaseParser:
    """Generic parsing functionality for any parser."""

    def __init__(self, parser_input: ParserInput):
        self.INPUTS = parser_input
        self.data = None
        self.err = []

        expected_classes = (dict, OrderedDict)
        if not isinstance(parser_input, expected_classes):
            self.err.append(
                ParserInitError(
                    f"Expected input's type to be one of {expected_classes}, got {type(parser_input)}"
                )
            )
            return

        try:
            # use Path obj to avoid introduction of extra backslashes,
            # don't know why but it happens on windows
            self.INPUTS["path"] = Path(parser_input["path"])
            self.INPUTS["buy"] = bool(parser_input["buy"])
        except KeyError:
            self.err.append(
                ParserInitError(
                    f"Expected required keys {get_type_hints(ParserInput).keys()}, but found {self.INPUTS.keys()}."
                )
            )
            return

        try:
            self._get_metadata()
        except Exception:
            self.err.append(
                ParserInitError(f"Unable to fetch metadata from {self.INPUTS=}")
            )
            return

    def erroed(self) -> bool:
        return bool(len(self.err))

    def _get_metadata(self):
        """Update the values of `self.xml`, `self.name` and `self.version`."""
        self.xml, self.name, self.version = {}, "COULD_NOT_GET_NAME", "COULD_NOT_GET_VERSION"
        try:
            with open(str(self.INPUTS["path"])) as doc:
                self.xml = xmltodict.parse(doc.read())
        except Exception as err:
            self.err.append(err)
        name = self._get_name(self.INPUTS["buy"])
        version = self._get_version()
        if name:
            self.name = name
        if version:
            self.version = version

    def _get_key(self, key: str):
        """
        Traverse the dictionary `d` looking for the specified `key`.

        ***Args***
            key: The key in `self.xml` to search for.

        ***Raises***
            KeyError: If `key` is not found at any level of `d`.

        ***Returns*** (any)
            The value associated to the first occurrence of `key` in `d`.
        """
        def get_dict_key(key, d=self.xml):
            if key in d.keys():
                return d[key]
            for val in d.values():
                if isinstance(val, dict):
                    dk = get_dict_key(key, d=val)
                    if dk:
                        return dk
            return None
        out = get_dict_key(key)
        if not out:
            self.err.append(KeyError(f"Key '{key}' wasn't found in the provided dictionary."))
        return out

    def _get_name(self, buy: bool) -> str | None:
        """Returns the name of"""
        try:
            if buy:
                return f"COMPRA {self._get_key("dest")["xNome"]}"
            else:
                return f"VENDA {self._get_key("emit")["xNome"]}"
        except Exception as err:
            self.err.append(err)
            return None

    def _get_version(self) -> str | None:
        """return a `str` with the version number of the document"""
        try:
            return self._get_key("nfeProc")["@versao"]
        except Exception as err:
            self.err.append(err)
            raise err


class FactParser(BaseParser):
    """
    Classe de objeto que contém os dados de uma nota fiscal .xml em formato
    de dicionário.
    """

    def _get_pay(self) -> PayInfo | None:
        """return the payment section of `self.xml` or `None` if failed."""
        try:
            pay = self._get_key("pag")
            if type(pay["detPag"]) is list:
                pay_types = [e["tPag"] for e in pay["detPag"]]
                pay_vals = [e["vPag"] for e in pay["detPag"]]
                return {
                    "type": convert_to_list_of_numbers(pay_types),
                    "amount": convert_to_list_of_numbers(pay_vals),
                }
            else:
                return {
                    "type": convert_to_list_of_numbers(pay["detPag"]["tPag"]),
                    "amount": convert_to_list_of_numbers(pay["detPag"]["vPag"]),
                }
        except Exception as err:
            self.err.append(f"Parsing failed at {__funcname__()}: {str(err)}")
            return None

    def _get_nfekey(self) -> KeyType | None:
        try:
            return self._get_key("@Id")[3:]
        except Exception:
            self.err.append(ParserParseError(f"Parsing failed at {__funcname__()}"))
            return None

    def _get_dt(self) -> datetime | None:
        try:
            dt = self._get_key("dhEmi")
            return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )
            return None

    def _get_total(self) -> TotalInfo | None:
        products, discount, taxes = "0", "0", "0"
        total = self._get_key("ICMSTot")
        if isinstance(total, dict):
            if "vNF" in total.keys():
                products = total["vNF"]
            if "vTotTrib" in total.keys():
                taxes = total["vTotTrib"]
            if "vDesc" in total.keys():
                discount = total["vDesc"]
        return {"products": products, "discount": discount, "taxes": taxes}

    def parse(self):
        key = self._get_nfekey()
        dt = self._get_dt()
        pay = self._get_pay()
        total = self._get_total()
        if None in (key, dt, pay, total):
            self.err.append(
                ParserParseError("Unable to fetch all data, validation will be skipped")
            )
            return

        try:
            self.data = FactRowElem(
                ChaveNFe=key,
                DataHoraEmi=dt,
                PagamentoTipo=pay["type"],
                PagamentoValor=pay["amount"],
                TotalProdutos=total["products"],
                TotalDesconto=total["discount"],
                TotalTributos=total["taxes"],
            )
        except Exception as err:
            self.err.append(
                ParserValidationError(f"Unable to validate data {str(err)}")
            )
