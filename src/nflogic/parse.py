from typing import TypedDict
from datetime import datetime
import xmltodict
import os


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


# PARSER
###############


class BaseParser:
    """"""
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
        try:
            xmlpath = parser_input["path"]
            buy = parser_input["buy"]
        except KeyError:
            self.erroed = True
            self.err = KeyError("Invalid input, expected key not found.")
            return

        try:
            with open(xmlpath) as doc:
                self.xml = xmltodict.parse(doc.read())
        except Exception as err:
            self.erroed = True
            self.err = err
            return
        
        try:
            self.name: str = self._get_name(buy)
        except Exception as _get_name_err:
            self.erroed = True
            self.err = _get_name_err
            return

        self.version = self._get_version()
        # --------------------------------------

    def __enter__(self):
        self.__init__()
        return self

    def __exit__(self, err_type, err_val, traceback):
        pass

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
