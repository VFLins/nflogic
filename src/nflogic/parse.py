from typing import TypedDict, get_type_hints
from datetime import datetime
from collections import OrderedDict
import xmltodict
import os
import re


SCRIPT_PATH = os.path.realpath(__file__)
BINDIR = os.path.join(os.path.split(SCRIPT_PATH)[0], "bin")


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


# VALIDATORS
###############


def valid_int(val: any):
    """
    Return `True` if `val` is of type `int` or coercible, `False` otherwise.
    """
    try:
        _ = int(val)
        return True
    except ValueError:
        return False


def valid_float(val: any):
    """
    Return `True` if `val` is of type `float` or coercible, `False` otherwise.
    """
    try:
        _ = float(val)
        return True
    except ValueError:
        return False


# DATA VALIDATION
###############


class KeyType(str):
    def __init__(self) -> None:
        super().__init__()


class DTType(datetime):
    def __init__(self) -> None:
        super().__init__()


class ListOfNumbersType(str):
    def __init__(self) -> None:
        super().__init__()


class FloatCoercibleType(str):
    def __init__(self) -> None:
        super().__init__()


class RowElem:
    """
    Generic class for validating parsed data. It's children shall:
    1. have annotated types in it's `self.__init__()`;
    2. store the `self.__init__()` arguments to `self`.
    """

    def _valid_key(self, key):
        if type(key) == str and len(key) == 44 and key.isdigit():
            return True
        return False

    def _valid_dt(self, dt):
        return type(dt) == datetime

    def _valid_list_of_numbers(self, string):
        pattern = r"^[0-9;.]+$"
        return bool(re.match(pattern, string))

    def _valid_float(self, floating_point):
        try:
            _ = float(floating_point)
            return True
        except ValueError:
            return False

    def _validate_all(self):
        types = self.__init__.__annotations__

        for var in types.keys():
            value = getattr(self, var)

            if types[var] == KeyType:
                if not self._valid_key(value):
                    raise ValueError(f"Invalid value {type(self)}.{var} = {value}")

            if types[var] == DTType:
                if not self._valid_dt(value):
                    raise ValueError(f"Invalid value in {var}: {value}")

            if types[var] == ListOfNumbersType:
                if not self._valid_list_of_numbers(value):
                    raise ValueError(f"Invalid value in {var}: {value}")

            if types[var] == FloatCoercibleType:
                if not self._valid_float(value):
                    raise ValueError(f"Invalid value in {var}: {value}")


# PARSER
###############


class ParserInitError(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class BaseParser:
    """Parser for data"""
    def __init__(self, parser_input: ParserInput):
        self.INPUTS = parser_input
        self.data = None
        self.xml = None
        self.name = None
        self.version = None

        self.erroed = False
        self.err = None

        # --------------------------------------
        # Should NOT RAISE on __init__
        expected_classes = (dict, OrderedDict)
        if not isinstance(parser_input, expected_classes):
            self.erroed = True
            self.err = ParserInitError(f"Expected input's type to be one of {expected_classes}, got {type(parser_input)}")
            return

        try:
            _, _ = parser_input["path"], parser_input["buy"]
        except KeyError:
            self.erroed = True
            self.err = ParserInitError(f"Expected required keys {get_type_hints(ParserInput).keys()}, but found {self.INPUTS.keys()}.")
            return

        try:
            self._get_metadata()
        except Exception:
            self.erroed = True
            self.err = ParserInitError(f"Unable to fetch metadata from {self.INPUTS=}")
            return
        # --------------------------------------

    class BaseRowElem(RowElem):
        def __init__(
            self,
            ChaveNFe: KeyType,
            DataHoraEmi: DTType,
            PagamentoTipo: ListOfNumbersType,
            PagamentoValor: ListOfNumbersType,
            TotalProdutos: FloatCoercibleType,
            TotalDesconto: FloatCoercibleType,
            TotalTributos: FloatCoercibleType,
        ):

            self.ChaveNFe = ChaveNFe
            self.DataHoraEmi = DataHoraEmi
            self.PagamentoTipo = PagamentoTipo
            self.PagamentoValor = PagamentoValor
            self.TotalProdutos = TotalProdutos
            self.TotalDesconto = TotalDesconto
            self.TotalTributos = TotalTributos

            self._validate_all()
            self.values = (
                self.ChaveNFe,
                self.DataHoraEmi.strftime("%Y-%m-%d %H:%M:%S %z"),
                self.PagamentoTipo,
                self.PagamentoValor,
                float(self.TotalProdutos),
                float(self.TotalDesconto),
                float(self.TotalTributos),
            )

    def __enter__(self):
        self.__init__()
        return self

    def __exit__(self, err_type, err_val, traceback):
        pass

    def _get_metadata(self):
        with open(self.INPUTS["path"]) as doc:
            self.xml = xmltodict.parse(doc.read())
        self.name: str = self._get_name(self.INPUTS["buy"])
        self.version = self._get_version()
        

    def _get_dict_key(self, d: dict, key):
        """
        Traverse the dictionary `d` looking for the specified `key`.

        ***Args***
            d (dict): The dictionary to search.
            key (any): The key to search for.

        ***Raises***
            KeyError: If `key` is not found at any level of `d`.

        ***Returns*** (any)
            The value associated to the first occurrence of `key` in `d`.
        """
        for k in d.keys():
            if k == key:
                return d[k]
            else:
                try:
                    if type(d[k]) == dict:
                        return self._get_dict_key(d[k], key=key)
                except KeyError:
                    continue
        raise KeyError("Key wasn't found in the provided dictionary.")

    def _get_name(self, buy: bool) -> str:
        try:
            if buy:
                return f"COMPRA {self._get_dict_key(self.xml, "dest")["xNome"]}"
            else:
                return f"VENDA {self._get_dict_key(self.xml, "emit")["xNome"]}"
        except Exception as err:
            self.erroed = True
            self.err = err
            return "ERROR_FETCHING_NAME"

    def _get_version(self) -> str | None:
        """return a `str` with the version number of the document"""
        try:
            return self._get_dict_key(self.xml, "nfeProc")["@versao"]
        except Exception as err:
            self.erroed = True
            self.err = err
            return None


class FactParser(BaseParser):
    """
    Classe de objeto que contém os dados de uma nota fiscal .xml em formato
    de dicionário.
    """

    def get_pay(self) -> PayInfo:
        """return the payment section of the `.xml` in ``"""
        try:
            pay = self._get_dict_key(self.xml, "pag")
        except KeyError:
            pay = self.xml["NFe"]["infNFe"]["pag"]
        if type(pay["detPag"]) is list:
            return {
                "type": ";".join([x["tPag"] if valid_int(x) else "NaN" for x in pay]),
                "amount": ";".join(
                    [x["vPag"] if valid_float(x) else "NaN" for x in pay]
                ),
            }
        else:
            return {"type": pay["detPag"]["tPag"], "amount": pay["detPag"]["vPag"]}

    def get_key(self) -> str:
        try:
            return self._get_dict_key(self.xml, "@Id")[3:]
        except Exception as err:
            self.erroed = True
            self.err = err

    def get_dt(self) -> datetime:
        dt = self._get_dict_key(self.xml, "dhEmi")
        return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")

    def get_total(self) -> TotalInfo:
        try:
            total = self.xml["nfeProc"]["NFe"]["infNFe"]["total"]["ICMSTot"]
        except KeyError:
            total = self.xml["NFe"]["infNFe"]["total"]["ICMSTot"]

        if valid_float(total["vNF"]):
            products = total["vNF"]

        if valid_float(total["vTotTrib"]):
            taxes = total["vTotTrib"]

        # expected to not appear sometimes, will not raise
        discount = "0"
        if "vDesc" in total.keys():
            if valid_float(total["vDesc"]):
                discount = total["vDesc"]

        return {"products": products, "discount": discount, "taxes": taxes}

    def parse(self):
        try:
            key = self.get_key()
            dt = self.get_dt()
            pay = self.get_pay()
            total = self.get_total()
        except Exception as err:
            self.erroed = True
            self.err = err
            return

        self.data = {
            "ChaveNFe": key,
            "DataHoraEmi": dt,
            "PagamentoTipo": pay["type"],
            "PagamentoValor": pay["amount"],
            "TotalProdutos": total["products"],
            "TotalDesconto": total["discount"],
            "TotalTributos": total["taxes"],
        }
