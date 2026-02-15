from typing import TypedDict, get_type_hints
from collections import OrderedDict
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
from copy import copy
import inspect
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


class ParserParseError(Exception):
    """Error while parsing the xml document."""


class ParserValidationError(Exception):
    """Error during validation of the data parsed from the xml document."""


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

            values.append(val)

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

    def __repr__(self):
        ChaveNFe = getattr(self, "ChaveNFe", None)
        DataHoraEmi = getattr(self, "DataHoraEmi", None)
        if DataHoraEmi:
            DataHoraEmi = DataHoraEmi.strftime("%Y-%m-%dT%H:%M:%S%z")
        return f"<{__name__}.FactRowElem: {ChaveNFe=}>"


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

    def __repr__(self):
        ChaveNFe = getattr(self, "ChaveNFe", None)
        CodProduto = getattr(self, "CodProduto", None)
        return f"<{__name__}.TransacRowElem: {ChaveNFe=}, {CodProduto=}>"


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
                    "Expected input's type to be one of "
                    f"{expected_classes}, got {type(parser_input)}"
                )
            )
            return

        try:
            _ = Path(parser_input["path"])
            self.INPUTS["buy"] = bool(parser_input["buy"])
        except KeyError:
            self.err.append(
                ParserInitError(
                    "Expected required keys "
                    f"{get_type_hints(ParserInput).keys()}, "
                    f"but found {self.INPUTS.keys()}."
                )
            )
            return
        except TypeError:
            self.err.append(
                ParserInitError(f"Invalid argument {parser_input['path']=}")
            )

        try:
            self._get_metadata()
        except Exception:
            self.err.append(
                ParserInitError(rf"Unable to fetch metadata from {self.INPUTS=}")
            )
            return

    def erroed(self) -> bool:
        return len(self.err) > 0

    def _get_metadata(self):
        """Update the values of `self.xml`, `self.name` and `self.version`."""
        self.xml = (BeautifulSoup(),)
        # use Path obj to avoid introduction of extra backslashes,
        # don't know why, but it happens on windows
        xml_path = str(Path(self.INPUTS["path"]))
        for encoding in ["utf-8", "iso-8859-1"]:
            try:
                with open(xml_path, encoding=encoding) as doc:
                    self.xml = BeautifulSoup(doc.read(), features="xml")
                break
            except UnicodeEncodeError:
                continue
        # Ensure name can be parsed
        _ = self.name
        # only append if every encoding failed
        if self.xml == {}:
            self.err.append(ExpatError("Parsing failed with every encoding attempt."))

    def _get_name(self, buyer: bool) -> str | None:
        """'COMPRA BUYER NAME' if `buyer==True`, 'VENDA SELLER NAME' otherwise."""
        try:
            metatag = "dest" if buyer else "emit"
            prefix = "COMPRA" if buyer else "VENDA"
            nametag = self.xml.find(metatag)
            if not nametag:
                return "COULD_NOT_GET_NAME"
            return f"{prefix} {nametag.find('xNome').text.upper()}"
        except Exception as err:
            self.err.append(err)
            return None

    @staticmethod
    def _get_nested_tag_text(obj: BeautifulSoup, *tags: str, default: any = None):
        """Retrieve the `text` property under the nested `tags` from `obj`, while
        handling errors. Return `default` if the value cannot be retrieved.
        """
        for tag in tags:
            try:
                obj = obj.find(tag)
            except AttributeError:
                return default
        return getattr(obj, "text", default)

    @property
    def name(self) -> str:
        return self._get_name(buyer=self.INPUTS["buy"])

    @property
    def doc_version(self) -> str:
        """Return a `str` with the version number of the document."""
        tag = self.xml.find("nfeProc", attrs={"versao": True})
        return tag["versao"] if tag is not None else "Unable to fetch version."

    @property
    def doc_nfekey(self) -> KeyType | None:
        """Return a `str` with the 'Chave NFe' of this document. This number is a
        unique identifier of the document.
        """
        # first try
        tag = self.xml.find("chNFe")
        if tag is not None:
            value = getattr(tag, "text", None)
            if value is not None:
                return value
        # second try
        tag = self.xml.find("infNFe", attrs={"Id": True})
        if tag is not None:
            return tag["Id"][3:]
        # third try
        tag = self.xml.find("Reference", attrs={"URI": True})
        if tag is not None:
            return tag["URI"][4:]
        self.err.append(ParserParseError(f"Could not get {__funcname__()}"))
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
            pay = self.xml.find("pag")("detPag")
            if not pay:
                raise ParserParseError("Could not find nested tags: pag > detPag")
            pay_types = [int(getattr(e.find("tPag"), "text", 0)) for e in pay]
            pay_vals = [float(getattr(e.find("vPag"), "text", 0)) for e in pay]
            return {
                "type": convert_to_list_of_numbers(pay_types),
                "amount": convert_to_list_of_numbers(pay_vals),
            }
        except Exception as err:
            self.err.append(f"Parsing failed at {__funcname__()}: {str(err)}")

    def _get_dt(self) -> datetime | None:
        try:
            dt = getattr(self.xml.find("dhEmi"), "text", None)
            return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

    def _get_total(self) -> TotalInfo | None:
        tagname = "ICMSTot"
        try:
            total = self.xml.find(tagname)
            if not total:
                raise ParserParseError(f"Could not get tag: {tagname}")
            return {
                "products": getattr(total.find("vNF"), "text", "0"),
                "discount": getattr(total.find("vDesc"), "text", "0"),
                "taxes": getattr(total.find("vTotTrib"), "text", "0"),
            }
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

    def _get_fact_rows(self):
        key = self.doc_nfekey
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
        try:
            return [
                {
                    "prod": getattr(product.find("cProd"), "text", ""),
                    "ean": getattr(product.find("cEAN"), "text", ""),
                    "eantrib": getattr(product.find("cEANTrib"), "text", ""),
                    "ncm": getattr(product.find("NCM"), "text", ""),
                    "cest": getattr(product.find("CEST"), "text", ""),
                    "cfop": getattr(product.find("CFOP"), "text", ""),
                }
                for product in products
            ]
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

    def _get_product_desc(self, products: list[BeautifulSoup]) -> list[str]:
        """
        Parses `products`' description text.

        :param products: List of dictionaries containig data from each product.
        :return: List of product names.
        """
        try:
            return [product.find("xProd").text for product in products]
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

    def _get_product_amount(self, products: list[BeautifulSoup]) -> list[dict[str:any]]:
        """
        Parses `products`' prices data (e.g. Number of items sold or taxed),
        with keys:

        - qcom: Number of items;
        - qtrib: Amount of items or subitems considered for taxation;
        - undcom: Identifier of _qcom_ items' packaging (e.g. box, blister, pack);
        - undtrib: Identifier of _qtrib_ items' packaging.

        :param products: List of dictionaries containig data from each product.
        :return: List of pricing data of each item in `products`.
        """
        try:
            return [
                {
                    "qcom": float(getattr(product.find("qCom"), "text", 0)),
                    "qtrib": float(getattr(product.find("qTrib"), "text", 0)),
                    "undcom": getattr(product.find("uCom"), "text", ""),
                    "undtrib": getattr(product.find("uTrib"), "text", ""),
                }
                for product in products
            ]
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

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
        try:
            return [
                {
                    "vund": float(
                        self._get_nested_tag_text(product, "vProd", default=0)
                    ),
                    "bpis": float(
                        self._get_nested_tag_text(product, "PIS", "vBC", default=0)
                    ),
                    "vpis": float(
                        self._get_nested_tag_text(product, "PIS", "vPIS", default=0)
                    ),
                    "bcofins": float(
                        self._get_nested_tag_text(product, "COFINS", "vBC", default=0)
                    ),
                    "vcofins": float(
                        self._get_nested_tag_text(
                            product, "COFINS", "vCOFINS", default=0
                        )
                    ),
                    "bricms": float(
                        self._get_nested_tag_text(product, "vBCSTRet", default=0)
                    ),
                    "vricms": float(
                        self._get_nested_tag_text(product, "vICMSSTRet", default=0)
                    ),
                    "vsicms": float(
                        self._get_nested_tag_text(product, "ICMSSubstituto", default=0)
                    ),
                    "bicms": float(
                        self._get_nested_tag_text(product, "vBCEfet", default=0)
                    ),
                    "vicms": float(
                        self._get_nested_tag_text(product, "vICMSEfet", default=0)
                    ),
                }
                for product in products
            ]
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

    def _get_transac_rows(self):
        with open(self.INPUTS["path"]) as xmldoc:
            soup = BeautifulSoup(xmldoc.read(), features="xml")
        products = soup("det")

        key = self.doc_nfekey
        codes = self._get_product_codes(products)
        amounts = self._get_product_amount(products)
        names = self._get_product_desc(products)
        txinfos = self._get_product_tax_info(products)
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
