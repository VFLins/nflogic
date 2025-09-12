from typing import TypedDict, get_type_hints, Type
from collections import OrderedDict
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
from copy import copy
import inspect
import xmltodict
from xml.parsers.expat import ExpatError
import os
import re


SCRIPT_PATH = os.path.realpath(__file__)
BINDIR = os.path.join(os.path.split(SCRIPT_PATH)[0], "bin")


def __funcname__():
    return inspect.stack()[1][3]


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
TranscParserData = TypedDict(
    "TransacParserData",
    {
        "ChaveNFe": str,
        "CodProduto": str,  # codes that might start with zero
        "CodBarras": str,  # are stored as strings
        "CodNCM": str,
        "CodCEST": str,
        "CodCFOP": int,
        "QuantComercial": float,
        "QuantTributavel": float,
        "UnidComercial": str,
        "UnidTributavel": str,
        "DescricaoProd": str,
        "ValorUnitario": float,
        "BaseCalcPIS": float,
        "ValorPIS": float,
        "BaseCalcCOFINS": float,
        "ValorCOFINS": float,
        "BaseCalcRetidoICMS": float,
        "ValorRetidoICMS": float,
        "ValorSubstitutoICMS": float,
        "BaseCalcEfetivoICMS": float,
        "ValorEfetivoICMS": float,
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
    """Error during initialization of the parser."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ParserParseError(Exception):
    """Error while parsing the xml document."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ParserValidationError(Exception):
    """Error during validation of the data parsed from the xml document."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


# DATA VALIDATION
###############


def convert_to_list_of_numbers(
    inp: list[float] | list[int] | float | int,
) -> ListOfNumbersType:
    if type(inp) is list:
        float_in_inp = any(isinstance(item, (float, str)) for item in inp)
        if float_in_inp:
            inp = [float(i) for i in inp]
        else:
            inp = [int(i) for i in inp]
    return str(inp).replace(",", ";").replace(" ", "").replace("[", "").replace("]", "")


def convert_from_list_of_numbers(inp: ListOfNumbersType) -> list[float] | list[int]:
    nums_list = inp.replace("[", "").replace("]", "").split(";")
    if "." not in inp:
        return [int(i) for i in nums_list]
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
    """
    Return `True` if the string in `val` can be converted to a list of
    numbers separated by semicolon, `False` otherwise.
    """
    val = val.replace(" ", "")
    # check if string contains only integer/decimal numbers and semicolons
    if not re.match(r"^(\d+(\.\d+)?)(;(\d+(\.\d+)?))*$", val):
        return False
    if val.startswith(";") or val.endswith(";"):
        return False
    return True


def valid_key(val) -> bool:
    if (type(val) is str) and (len(val) == 44) and val.isdigit():
        return True
    return False


class RowElem:
    """
    Generic class for validating parsed data. It's children must:
    1. Have annotated variable names with the corresponding data type
    2. Run `super().__init__(**kwargs)` where kwargs are the parameters
      specified in `self.__init__()`
    """

    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            if name in self.__init__.__annotations__.keys():
                self.__setattr__(name, value)
        self.values = self._validate_and_assign()

    def _validate_and_assign(self):
        """
        Validate each piece of data by the it's annotated type and returns a
        tuple. Relies on a `self.__init__()` with type annotated parameters.

        Returns
            A tuple of the data provided in `self.__init__()`

        Raises
            ValueError if any piece of data do not conform to it's annotated
            type requirements
        """
        types = self.__init__.__annotations__
        values = []

        for var in types.keys():
            val = getattr(self, var)

            if types[var] == KeyType:
                if not valid_key(val):
                    raise ValueError(f"Invalid value {var}: {val}")
                values.append(val)
                continue

            if types[var] == datetime:
                if type(val) != datetime:
                    raise ValueError(f"Invalid value in {var}: {val}")
                values.append(val.strftime("%Y-%m-%d %H:%M:%S %z"))
                continue

            if types[var] == ListOfNumbersType:
                if not valid_list_of_numbers(val):
                    raise ValueError(f"Invalid value in {var}: {val}")
                values.append(val)
                continue

            if types[var] == FloatCoercibleType:
                if not valid_float(val):
                    raise ValueError(f"Invalid value in {var}: {val}")
                values.append(float(val))
                continue

        return tuple(values)


class FactRowElem(RowElem):
    """
    Validates and holds row data for a FactParser. See parent class for more details.
    """

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
        vars_dict = copy(vars())
        del vars_dict["self"]
        super().__init__(**vars_dict)


class TransacRowElem(RowElem):
    """
    Validates and holds row data for a TransacParser. See parent class for
    more details.
    """

    def __init__(
        self,
        ChaveNFe: KeyType,
        CodProduto: str,
        CodBarras: str,
        CodNCM: str,
        CodCEST: str,
        CodCFOP: str,
        QuantComercial: FloatCoercibleType,
        QuantTributavel: FloatCoercibleType,
        UnidComercial: str,
        UnidTributavel: str,
        DescricaoProd: str,
        ValorUnitario: FloatCoercibleType,
        BaseCalcPIS: FloatCoercibleType,
        ValorPIS: FloatCoercibleType,
        BaseCalcCOFINS: FloatCoercibleType,
        ValorCOFINS: FloatCoercibleType,
        BaseCalcRetidoICMS: FloatCoercibleType,
        ValorRetidoICMS: FloatCoercibleType,
        ValorSubstitutoICMS: FloatCoercibleType,
        BaseCalcEfetivoICMS: FloatCoercibleType,
        ValorEfetivoICMS: FloatCoercibleType,
    ):
        vars_dict = copy(vars())
        del vars_dict["self"]
        super().__init__(**vars_dict)


# PARSER
###############


class BaseParser:
    """Generic parsing functionality for any parser."""

    def __init__(self, parser_input: ParserInput):
        self.INPUTS = parser_input
        self.data = []
        self.err = []

        expected_classes = (dict, OrderedDict)
        if not isinstance(parser_input, expected_classes):
            self.err.append(
                ParserInitError(
                    f"Expected input's type to be one of {
                        expected_classes}, got {type(parser_input)}"
                )
            )
            return

        try:
            _ = Path(parser_input["path"])
            self.INPUTS["buy"] = bool(parser_input["buy"])
        except KeyError:
            self.err.append(
                ParserInitError(
                    f"Expected required keys {
                        get_type_hints(ParserInput).keys()}, "
                    f"but found {self.INPUTS.keys()}."
                )
            )
            return

        try:
            self._get_metadata()
        except Exception:
            self.err.append(
                ParserInitError(rf"Unable to fetch metadata from {self.INPUTS=}")
            )
            return

    def erroed(self) -> bool:
        return bool(len(self.err))

    def _get_metadata(self):
        """Update the values of `self.xml`, `self.name` and `self.version`."""
        self.xml, self.name, self.version = (
            {},
            "COULD_NOT_GET_NAME",
            "COULD_NOT_GET_VERSION",
        )
        # use Path obj to avoid introduction of extra backslashes,
        # don't know why, but it happens on windows
        xml_path = str(Path(self.INPUTS["path"]))
        for encoding in ["utf-8", "iso-8859-1"]:
            try:
                with open(xml_path) as doc:
                    self.xml = xmltodict.parse(doc.read(), encoding=encoding)
                break
            except ExpatError:
                continue
        # only append if every encoding failed
        if self.xml == {}:
            self.err.append(ExpatError("Parsing failed with every encoding attempt."))
        name = self._get_name(self.INPUTS["buy"])
        version = self._get_version()
        if name:
            self.name = name
        if version:
            self.version = version

    def _get_key(self, key: str, dictionary: dict):
        """
        Traverse `dictionary` looking for the specified `key`.

        :param key: The key to search for.
        :param dictionary: Dictionary that will be traversed.
        :raises KeyError: If `key` is not found at any level of `dictionary`.
        :returns: The value associated to the first occurrence of `key` in `dictionary`.
        """
        def get_dict_key(key, d=dictionary):
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
            self.err.append(
                KeyError(f"Key '{key}' wasn't found in the provided dictionary.")
            )
        return out

    def _get_all_keys(self, key, dictionary):
        """
        Traverse `dictionary` fetching all ocurrences of `key`, and return a list of
        matches.

        :param key: The key to search for.
        :param dictionary: Dictionary that will be traversed.
        :raises KeyError: If `key` is not found at any level of `dictionary`.
        :returns: The value associated to the first occurrence of `key` in `dictionary`.
        """
        def yield_dict_key(key: str, d: dict):
            result = []
            if key in d.keys():
                result.append(d[key])
            for val in d.values():
                if isinstance(val, dict):
                    dk = yield_dict_key(key, d=val)
                    if dk:
                        result = result + [i for i in dk]
            yield result
        out = [i for i in yield_dict_key(key=key, d=dictionary)]
        if len(out) == 0:
            self.err.append(
                KeyError(f"No instance of {key=} found in the provided dictionary.")
            )
        return out

    def _get_name(self, buy: bool) -> str | None:
        """Return 'COMPRA {BUYER_NAME}' if `buy==True`, 'VENDA {SELLER_NAME}' otherwise."""
        try:
            if buy:
                return f"COMPRA {self._get_key('dest', dictionary=self.xml)['xNome']}"
            return f"VENDA {self._get_key('emit', dictionary=self.xml)['xNome']}"
        except Exception as err:
            self.err.append(err)
            return None

    def _get_version(self) -> str | None:
        """return a `str` with the version number of the document"""
        try:
            return self._get_key("@versao", dictionary=self.xml)
        except Exception as err:
            self.err.append(err)
            raise err

    def _get_nfekey(self) -> KeyType | None:
        try:
            return self._get_key("@Id", dictionary=self.xml)[3:]
        except Exception:
            self.err.append(ParserParseError(f"Parsing failed at {__funcname__()}"))
            return None

    def _parsed(self) -> bool:
        """Informs if `self.parse()` was ever called."""
        return self.erroed() or bool(len(self.data))


class FactParser(BaseParser):
    """
    Classe de objeto que contém os dados de uma nota fiscal .xml em formato
    de dicionário.
    """

    def _get_pay(self) -> PayInfo | None:
        """return the payment section of `self.xml` or `None` if failed."""
        try:
            pay = self._get_key("pag", dictionary=self.xml)
            if type(pay["detPag"]) is list:
                pay_types = [float(e["tPag"]) for e in pay["detPag"]]
                pay_vals = [float(e["vPag"]) for e in pay["detPag"]]
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

    def _get_dt(self) -> datetime | None:
        try:
            dt = self._get_key("dhEmi", dictionary=self.xml)
            return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )
            return None

    def _get_total(self) -> TotalInfo | None:
        products, discount, taxes = "0", "0", "0"
        total = self._get_key("ICMSTot", dictionary=self.xml)
        if isinstance(total, dict):
            if "vNF" in total.keys():
                products = total["vNF"]
            if "vTotTrib" in total.keys():
                taxes = total["vTotTrib"]
            if "vDesc" in total.keys():
                discount = total["vDesc"]
        return {"products": products, "discount": discount, "taxes": taxes}

    def _get_fact_rows(self):
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
            out = []
            out.append(
                FactRowElem(
                    ChaveNFe=key,
                    DataHoraEmi=dt,
                    PagamentoTipo=pay["type"],
                    PagamentoValor=pay["amount"],
                    TotalProdutos=total["products"],
                    TotalDesconto=total["discount"],
                    TotalTributos=total["taxes"],
                )
            )
            return out
        except Exception as err:
            self.err.append(
                ParserValidationError(f"Unable to validate data {str(err)}")
            )

    def parse(self):
        if self._parsed():
            return
        rows = self._get_fact_rows()
        if rows is not None:
            self.data = self.data + rows


class _TransacParser(BaseParser):
    def _get_product_codes(self, products: list[dict]) -> list[dict[str, str]]:
        """
        Parses `products`' tax classification codes (NCM, CEST and CFOP), and
        other codes used for identification (CODE and EAN), with keys:

        - prod: Product identifier on seller-side system;
        - ean: Barcode displayed in product's package;
        - ncm: 'Nomenclatura Comum do Mercosul';
        - cest: 'Código Especificador da Substituição Tributária';
        - cfop: 'Código Fiscal de Operações e Prestações'.

        :param products: List of dictionaries containig data from each product.
        :return: List of code values for each `product` in the provided order.
        """
        return [
            {
                "prod": product.find("cProd").text,
                "ean": product.find("cEAN").text,
                "eantrib": product.find("cEANTrib").text,
                "ncm": product.find("NCM").text,
                "cest": product.find("CEST").text,
                "cfop": product.find("CFOP").text,
            }
            for product in products
        ]

    def _get_product_desc(self, products: list[BeautifulSoup]) -> list[str]:
        """
        Parses `products`' description text.

        :param products: List of dictionaries containig data from each product.
        :return: List of product names.
        """
        return [product.find("xProd").text for product in products]

    def _get_product_amount(self, products: list[BeautifulSoup]) -> list[dict[str: any]]:
        """
        Parses `products`' prices data (e.g. Number of items sold or taxed),
        with keys:

        - qcom: Number of items;
        - qtrib: Amount of items or subitems considered for taxation;
        - undcom: Identifier of _qcom_ items packaging (e.g. box, blister, pack);
        - undtrib: Identifier of _qtrib_ items packaging.

        :param products: List of dictionaries containig data from each product.
        :return: List of pricing data of each item in `products`.
        """
        return [
            {
                "qcom": float(product.find("qCom").text),
                "qtrib": float(product.find("qTrib").text),
                "undcom": product.find("uCom").text,
                "undtrib": product.find("uTrib").text,
            }
            for product in products
        ]

    def _get_product_tax_info(self, products: list[BeautifulSoup]):
        """
        Parses `products` for taxation information besides amount of items
        (see _get_product_amount()) and price (e.g. unitary price, cost of any
        applicable tax), with keys:

        - vund: Price of one item of _qcom_;
        - bpis: Amount on which the 'PIS' tax calculation is based;
        - vpis: Amount collected for 'PIS' tax;
        - bcofins: Amount on which the 'COFINS' tax calculation is based;
        - vcofins: Amount collected for 'COFINS' tax;
        - bricms: Amount on which 'ICMS' tax calculation was based previously in the
          production chain;
        - vricms: Amount previously collected for 'ICMS' tax in the production chain;
        - vsicms: Amount collected for 'ICMS' tax by this seller;
        - bicms: Amount on which the 'ICMS' tax calculation is based;
        - vicms: Total amount collected for 'ICMS' tax.

        :param products: List of dictionaries containig data from each product.
        :return: List of taxation data of each item in `products`.
        """
        return [
            {
                "vund": float(product.find("vProd").text),
                "bpis": float(getattr(product.find("PIS").find("vBC"), "text", 0)),
                "vpis": float(getattr(product.find("PIS").find("vPIS"), "text", 0)),
                "bcofins": float(getattr(product.find("COFINS").find("vBC"), "text", 0)),
                "vcofins": float(getattr(product.find("COFINS").find("vCOFINS"), "text", 0)),
                "bricms": float(getattr(product.find("vBCSTRet"), "text", 0)),
                "vricms": float(getattr(product.find("vICMSSTRet"), "text", 0)),
                "vsicms": float(getattr(product.find("vICMSSubstituto"), "text", 0)),
                "bicms": float(getattr(product.find("vBCEfet"), "text", 0)),
                "vicms": float(getattr(product.find("vICMSEfet"), "text", 0)),
            }
            for product in products
        ]

    def _get_transac_rows(self):
        soup = BeautifulSoup(self.INPUTS["path"], features="xml")
        products = soup("det")

        key = self._get_nfekey()
        codes = self._get_product_codes()
        amounts = self._get_product_amount()
        names = self._get_product_desc()
        txinfos = self._get_product_tax_info()
        if None in (key, codes, amounts, names, txinfos):
            self.err.append(
                ParserParseError("Unable to fetch all data, validation will be skipped")
            )
            return
        try:
            out = []
            for code, amount, name, txinfo in zip(codes, amounts, names, txinfos):
                out.append(
                    TransacRowElem(
                        ChaveNFe=key,
                        CodProduto=code["prod"],
                        CodBarras=code["ean"],
                        CodNCM=code["ncm"],
                        CodCEST=code["cest"],
                        CodCFOP=code["cfop"],
                        QuantComercial=amount["qcom"],
                        QuantTributavel=amount["qtrib"],
                        UnidComercial=amount["undcom"],
                        UnidTributavel=amount["undtrib"],
                        DescricaoProd=name,
                        ValorUnitario=txinfo["vund"],
                        BaseCalcPIS=txinfo["bpis"],
                        ValorPIS=txinfo["vpis"],
                        BaseCalcCOFINS=txinfo["bcofins"],
                        ValorCOFINS=txinfo["vcofins"],
                        BaseCalcRetidoICMS=txinfo["bricms"],
                        ValorRetidoICMS=txinfo["vricms"],
                        ValorSubstitutoICMS=txinfo["vsicms"],
                        BaseCalcEfetivoICMS=txinfo["bicms"],
                        ValorEfetivoICMS=txinfo["vicms"],
                    )
                )
            return out
        except Exception as err:
            self.err.append(
                ParserValidationError(f"Unable to validate data {str(err)}")
            )

    def parse(self):
        if self._parsed():
            return
        rows = self._get_transac_rows()
        if rows is not None:
            self.data = self.data + rows


class FullParser(FactParser, _TransacParser):
    def parse(self):
        if self._parsed():
            return
        fact_rows = self._get_fact_rows()
        transac_rows = self._get_transac_rows()
        if fact_rows is not None:
            self.data = self.data + fact_rows
        if transac_rows is not None:
            self.data = self.data + transac_rows
