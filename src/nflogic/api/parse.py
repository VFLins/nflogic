from typing import TypedDict, Any, get_type_hints
from collections import OrderedDict
from datetime import datetime
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
from pathlib import Path
from copy import copy
import inspect
from xml.parsers.expat import ExpatError
import os
import re

SCRIPT_PATH = os.path.realpath(__file__)
"""@private Caminho para este *script*, não alterar."""

BINDIR = os.path.join(os.path.split(SCRIPT_PATH)[0], "bin")
"""@private Na realidade eu não sei para que isto serve."""


def __funcname__():
    return inspect.stack()[1][3]


# TYPES
###############


class ParserInput(TypedDict):
    """*Dicionário* do Python com tipos definidos, usado como input padrão para
    qualquer *parser*.
    """

    path: str
    """Caminho para o arquivo que deve ser processado."""

    buy: bool
    """Indica se o arquivo deve ser considerado uma nota de compra, caso
    contrário, será uma nota de venda.
    """


class PayInfo(TypedDict):
    """*Dicionário* do Python com tipos definidos, usado apenas internamente pelos
    *parsers* ao processar informações de pagamento.
    """

    type: str
    """Equivalente à `FactParserData.PagamentoTipo`."""

    amount: str
    """Equivalente à `FactParserData.PagamentoValor`."""


class TotalInfo(TypedDict):
    """*Dicionário* do Python com tipos definidos, usado apenas internamente pelos
    *parsers* ao processar informações de valores totais no documento.
    """

    products: float
    """Equivalente à `FactParserData.TotalProdutos`."""

    discount: float
    """Equivalente à `FactParserData.TotalDesconto`."""

    taxes: float
    """Equivalente à `FactParserData.TotalTributos`."""


class FactParserData(TypedDict):
    """Estrutura de dados que deve ser transferido de um *parser* para o banco de
    dados. Pode ser usado como referência documental para as tabelas *fato* produzidas
    pelo NF-Logic.
    """

    ChaveNFe: str
    """Número oficial de identificação da nota fiscal com a receita federal."""

    DataHoraEmi: datetime
    """Data e hora de emissão do documento, inclui indicação de fuso horário."""

    PagamentoTipo: str
    """Indica as formas de pagamento usadas, separados por `;`."""

    PagamentoValor: str
    """Indica os valores pagos em cada forma de pagamento, separados por `;`."""

    TotalProdutos: float
    """Valor total dos produtos ou serviços."""

    TotalDesconto: float
    """Valor total dos descontos concedidos."""

    TotalTributos: float
    """Valor total dos tributos cobrados."""


class TransacParserData(TypedDict):
    """Estrutura de dados que deve ser transferido de um *parser* para o banco de
    dados. Pode ser usado como referência documental para as tabelas *transação*
    produzidas pelo NF-Logic.
    """

    ChaveNFe: str
    """Número oficial de identificação da nota fiscal com a receita federal."""

    CodProduto: str
    """Código de identificação do produto, gerado interna e independentemente pelo
    sistema de estoque do vendedor. Registrado como *String* porque pode começar com
    zeros.
    """

    CodBarras: str
    """Código de barras único do produto cadastrado pelo Cadastro Nacional de Produtos
    (CNP). Registrado como *String* porque pode começar com zeros.
    """

    CodNCM: str
    """Código da Nomenclatura Comum do Mercosul (NCM) do produto, usado para
    categorizar tributariamente os produtos. Registrado como *String* porque pode
    começar com zeros.
    """

    CodCEST: str
    """Código Especificador da Substituição Tributária, usado para identificar produtos
    sujeitos à substituição tributária. Registrado como *String* porque pode começar
    com zeros.
    """

    CodCFOP: int
    """Código Fiscal de Operações e Prestações (CFOP), epecificando a natureza da
    circulação desta mercadoria.
    """

    QuantComercial: float
    """Quantidade informada na venda ao cliente."""

    QuantTributavel: float
    """Quantidade informada na declaração tributária."""

    UnidComercial: str
    """Unidade (ex.: fardos, litros, caixas) informada na venda ao cliente."""

    UnidTributavel: str
    """Unidade (ex.: fardos, litros, caixas) informada na declaração tributária."""

    DescricaoProd: str
    """Texto descrevendo o produto, definido internamente pelo sistema de estoque do
    vendedor.
    """

    ValorUnitario: float
    """Preço em reais de uma unidade comercial (`TransacParserData.UnidComercial`)."""

    BaseCalcPIS: float
    """Valor em que o tributo do Programa de Integração Social (PIS) incide."""

    ValorPIS: float
    """Valor recolhido $V$ para o tributo do Programa de Integração Social (PIS).
    A alíquota pode ser obitda com este valor dividido por sua base de cálculo $B_c$,
    quando esta for diferente de zero:

    $$Alíq. = \\frac{V}{B_c}$$

    O mesmo serve para os demais tributos abaixo.
    """

    BaseCalcCOFINS: float
    """Valor em que o tributo da Contribuição para o Financiamento da Seguridade
    Social (COFINS) incide.
    """

    ValorCOFINS: float
    """Valor recolhido para o tributo da Contribuição para o Financiamento da
    Seguridade Social (COFINS).
    """

    BaseCalcRetidoICMS: float
    """Equivalente ao somatório das bases de cálculo anteriores do Imposto sobre
    Circulação de Mercadorias e Prestação de Serviços (ICMS) incidentes nas etapas
    anteriores da cadeia de produção.
    """

    ValorRetidoICMS: float
    """Valor do Imposto sobre Circulação de Mercadorias e Prestação de Serviços (ICMS)
    pago anteriormente com base no `TransacParserData.BaseCalcRetidoICMS`.
    """

    ValorSubstitutoICMS: float
    """Valor estimado do Imposto sobre Circulação de Mercadorias e Prestação de
    Serviços (ICMS) previsto antecipadamente no início da cadeia de distribuição.
    """

    BaseCalcEfetivoICMS: float
    """Valor real em que o Imposto sobre Circulação de Mercadorias e Prestação de
    Serviços (ICMS) incide na etapa atual da cadeia de distribuição, mas apenas em
    caso de venda para o consumidor final.
    """

    ValorEfetivoICMS: float
    """Valor do Imposto sobre Circulação de Mercadorias e Prestação de Serviços (ICMS)
    recolhível com base no `TransacParserData.BaseCalcEfetivoICMS`.
    """


class KeyType(str):
    """Tipo customizado de *String*, usado para validação de dados armazenados como
    texto, mas representam um código único identificador de nota fiscal. Exemplos:
    `FactParserData.ChaveNFe` e `TransacParserData.ChaveNFe`.
    """

    def __init__(self) -> None:
        super().__init__()


class ListOfNumbersType(str):
    """Tipo customizado de *String*, usado para validação de dados armazenados como
    texto, mas que podem ser convertidos em uma sequência de números. Exemplos:
    `FactParserData.PagamentoTipo` e `FactParserData.PagamentoValor`.
    """

    def __init__(self) -> None:
        super().__init__()


class FloatCoercibleType(str):
    """Tipo customizado de *String*, usado para validar dados armazenados neste
    formato, mas que ainda podem ser convertidos em *Float*
    """

    def __init__(self) -> None:
        super().__init__()


class ParserInitError(Exception):
    """Erro lançado durante a inicialização de um *parser*."""


class ParserParseError(Exception):
    """Erro lançado ao tentar coletar os dados de um documento."""


class ParserValidationError(Exception):
    """Erro lançado ao validar os dados coletados de um documento."""


# DATA VALIDATION
###############


def convert_to_list_of_numbers(
    inp: list[float] | list[int] | float | int,
) -> ListOfNumbersType:
    """Função de conversão.

    :param inp: Uma *Lista de números* ou um *Número* único

    :return: *String* identificável como `ListOfNumbersType`.
    """
    if type(inp) is list:
        float_in_inp = any(isinstance(item, (float, str)) for item in inp)
        if float_in_inp:
            inp = [float(i) for i in inp]
        else:
            inp = [int(i) for i in inp]
    return str(inp).replace(",", ";").replace(" ", "").replace("[", "").replace("]", "")


def convert_from_list_of_numbers(inp: ListOfNumbersType) -> list[float] | list[int]:
    """Função de conversão.

    :param inp: *String* identificável como `ListOfNumbersType`.

    :return: Uma *Lista de números*
    """
    nums_list = inp.replace("[", "").replace("]", "").split(";")
    if "." not in inp:
        return [int(i) for i in nums_list]
    return [float(i) for i in nums_list]


def valid_int(val: Any) -> bool:
    """Verifica se `val` pode ser convertido para um número inteiro.

    :param val: Valor a ser verificado.

    :return: Um valor *Booleano* com a resposta da verificação.
    """
    try:
        _ = int(val)
        return True
    except ValueError:
        return False


def valid_float(val: any) -> bool:
    """Verifica se `val` pode ser convertido para um número não inteiro.

    :param val: Valor a ser verificado.

    :return: Um valor *Booleano* com a resposta da verificação.
    """
    try:
        _ = float(val)
        return True
    except ValueError:
        return False


def valid_list_of_numbers(val: str) -> bool:
    """Verifica se `val` pode ser convertido para uma *String* identificável como
    `ListOfNumbersType`.

    :param val: Valor a ser verificado.

    :return: Um valor *Booleano* com a resposta da verificação.
    """
    val = val.replace(" ", "")
    # check if string contains only integer/decimal numbers and semicolons
    if not re.match(r"^(\d+(\.\d+)?)(;(\d+(\.\d+)?))*$", val):
        return False
    if val.startswith(";") or val.endswith(";"):
        return False
    return True


def valid_key(val) -> bool:
    """Verifica se `val` pode ser convertido para uma *String* identificável como
    `KeyType`.

    :param val: Valor a ser verificado.

    :return: Um valor *Booleano* com a resposta da verificação.
    """
    if (type(val) is str) and (len(val) == 44) and val.isdigit():
        return True
    return False


class RowElem:
    """
    Classe genérica que valida os dados recebidos durante a inicialização. Esta
    validação não pode ser feita diretamente nesta classe, ela precisa que uma classe
    herdeira especifique as variáveis e seus respectivos tipos, e depois execute o
    `__init__()` da classe pai. Exemplo:

    .. code-block:: python
        class ExampleRow(RowElem):
            def __init__(self, key: KeyType, numbers: ListOfNumbersType, generic):
                super().__init__(key=key, numbers=numbers, generic=generic)

    Neste exemplo, o parâmetro:
    - `key` é validado por `is_key()`, por ter a anotação de tipo `KeyType`,
    - `numbers` é validado por `is_list_of_numbers()` por ter anotação de tipo
        `ListOfNumbersType`.
    - `generic` não passa por nenhum tipo de validação, por não receber anotação de
        tipo.

    Veja o código de `RowElem._validate_and_assign()` para ver todos os tipos de
    validação consideradas.
    """

    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            if name in self.__init__.__annotations__.keys():
                self.__setattr__(name, value)
        self.values = self._validate_and_assign()
        """Dados inseridos neste `RowElem` devidamente validados e convertidos para o
        formato esperado pelo banco de dados.
        """

    def _validate_and_assign(self) -> tuple:
        """@public
        Valida os dados recebidos de `RowElem.__init__()`, passado

        :return: Uma *Tupla* com os dados validados para serem atribuídos à
            `RowElem.values`.

        :raises ValueError: Se qualquer dado fornecido não for válido.
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
    """Valida e converte dados para serem inseridos em tabelas *fato* do banco de
    dados. Veja `RowElem` para mais detalhes.
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
    """Valida e converte dados para serem inseridos em tabelas *transação* do banco de
    dados. Veja `RowElem` para mais detalhes.
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
    """
    Oferece as funcionalidades básicas de um *parser*, mas como não implementa um
    método `BaseParser.parse()`, não tem capacidade de processar nenhum documento.
    Todos os *parsers* devem herdar seus métodos e atributos.

    .. note:: Erros são registrados silenciosamente em `BaseParser.err`.
        Os métodos de *parsers* são levantados silenciosamente para não interromper
        execuções consecutivas, em que muitos arquivos precisam ser processados.
        Embora os métodos indiquem "Erros levantados", estes erros são silenciosos e
        não aparecerão no *stdout* durante a execução deste *parser*.
    """

    def __init__(self, parser_input: ParserInput):
        """Fornece funcionalidades básicas para *parsers*.

        :param parser_input: Um objeto compatível com `ParserInput`.
        """
        self.INPUTS: ParserInput = parser_input
        """Objeto fornecido no parâmetro `parser_input` ao instanciar esta classe."""
        self.data = []
        """*Lista de subclassses de `RowElem`* contendo dados processados por este
        *parser*, estará vazia enquanto nenhum dado tiver sido obtido.
        """

        self.err = []
        """*Lista de Exceções* recebidas. Todos os erros são levantados
        silenciosamente e armazenados aqui por ordem de chegada.
        """

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
        """Indica se algum erro já foi levantado por este *parser*.

        :return: Um valor *Booleano* com a resposta da verificação.
        """
        return len(self.err) > 0

    def _get_metadata(self):
        """Atualiza os valores de `self.xml`, `self.name` e `self.version`."""
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
        """Retorna o nome usado para registrar dados deste parser no banco de dados ou
        cache de erros.

        :param buyer: Indica o nome que será extraído da nota e qual prefixo aparecerá
            antes dele
            - 'COMPRA [NOME_COMPRADOR]', caso verdadeiro, ou
            - 'VENDA [NOME_VENDEDOR]' caso contrário.

        :return: Uma *String* com o nome coletado, ou `None` caso não seja possível.
        """
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
    def _get_nested_tag_text(
        obj: BeautifulSoup, *tags: str, default: any = None
    ) -> str:
        """Tenta obter o texto contido imediatamente dentro do caminho de `tags` aninhadas
        fornecido.

        :param obj: Um objeto `BeautifulSoup`.
        :param *tags: Caminho de tags a ser seguido para obter o texto.
        :param default: Valor padrão que será obtido caso o texto não seja encontrado.

        :return: Uma *String* com o texto coletado.
        """
        for tag in tags:
            try:
                obj = obj.find(tag)
            except AttributeError:
                return default
        return getattr(obj, "text", default)

    @property
    def name(self) -> str | None:
        """Nome usado para registrar dados deste parser no banco de dados ou cache de
        erros. Será `None` quando não for possível obter um nome.
        """
        return self._get_name(buyer=self.INPUTS["buy"])

    @property
    def doc_version(self) -> str:
        """Número de versão do documento."""
        tag = self.xml.find("nfeProc", attrs={"versao": True})
        return tag["versao"] if tag is not None else "Unable to fetch version."

    @property
    def doc_nfekey(self) -> KeyType | None:
        """Identificador único deste documento 'Chave NFe'."""
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
        """Informa se `.parse()` foi executado alguma vez.

        :return: Um valor *Booleano* com a resposta da verificação.
        """
        return self.erroed() or bool(len(self.data))


class FactParser(BaseParser):
    """*Parser* dedicado à extrair apenas metadados e informações de pagamento do
    documento XML fornecido. Usado para preencher dados de tabelas *fato* no banco de
    dados.

    Veja `BaseParser` para mais informações.
    """

    def _get_pay(self) -> PayInfo | None:
        """Retorna dados de pagamento do documento, ou `None`, se não for possível."""
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
        """Retorna a data de emissão do documento, ou `None`, se não for possível."""
        try:
            dt = getattr(self.xml.find("dhEmi"), "text", None)
            return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

    def _get_total(self) -> TotalInfo | None:
        """Retorna os valores relevantes da operação econômica relevante, ou `None`, se
        não for possível.
        """
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

    def _get_fact_rows(self) -> FactRowElem | None:
        """Função ajudante que coleta os dados relevantes para a tabela *fato*.

        :return: Um `FactRowElem` com os dados coletados, ou `None` caso não seja
            possível.
        :raises ParserParseError: Quando a coleta de dados não é possível
        """
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
        """Processa o documento e, caso tenha sucesso, vai popular `FactParser.data`,
        caso contrário, os erros serão registrados em `FactParser.err`.
        """
        if self._parsed():
            return
        rows = self._get_fact_rows()
        if rows is not None:
            self.data = self.data + rows


class _TransacParser(BaseParser):
    """@public *Parser* dedicado à extrair apenas metadados e informações de produtos
    e serviços incluídos na operação comercial do documento XML fornecido. Usado para
    preencher dados de tabelas *transação* no banco de dados.

    Veja `BaseParser` para mais informações.
    """

    def _get_product_codes(self, products: ResultSet[Tag]) -> list[dict[str, str]]:
        """Coleta códigos que identificam e classificam cada produto desta nota fiscal.

        - prod: Número identificador do produto no sistema do vendedor;
        - ean: Código de barras do produto;
        - ncm: 'Nomenclatura Comum do Mercosul';
        - cest: 'Código Especificador da Substituição Tributária';
        - cfop: 'Código Fiscal de Operações e Prestações'.

        :param products: *Lista de bs4.BeautifulSoup*, cada uma com um segmento de
            dados de um produto.

        :return: *Lista dicionários* contendo os dados informados acima, ou `None` caso
            não seja possível.
        :raises ParserParseError: Se não for possível obter todos os dados.
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

    def _get_product_desc(self, products: ResultSet[Tag]) -> list[str] | None:
        """Coleta os textos de descrição de produto desta nota fiscal.

        :param products: *Lista de bs4.BeautifulSoup*, cada uma com um segmento de
            dados de um produto.

        :return: *Lista strings* contendo os dados informados acima, ou `None` caso
            não seja possível.
        :raises ParserParseError: Se não for possível obter todos os dados.
        """
        try:
            return [product.find("xProd").text for product in products]
        except Exception as err:
            self.err.append(
                ParserParseError(f"Parsing failed at {__funcname__()}: {str(err)}")
            )

    def _get_product_amount(
        self, products: ResultSet[Tag]
    ) -> list[dict[str, str | float]] | None:
        """Coleta dados de quantidade dos produtos ou serviços nesta nota fiscal.

        - qcom: Quantidade comercializada;
        - qtrib: Quantidade considerada na tributação;
        - undcom: Identificador do tipo de empactoamento em *qcom* (ex.: CX, UN, DZ);
        - undtrib: Identificador do tipo de empacotamento em *qtrib*.

        :param products: *Lista de bs4.BeautifulSoup*, cada uma com um segmento de
            dados de um produto.

        :return: *Lista de dicionários* contendo os dados informados acima, ou `None`
            caso não seja possível.
        :raises ParserParseError: Se não for possível obter todos os dados.
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

    def _get_product_tax_info(
        self, products: ResultSet[Tag]
    ) -> list[dict[str, float]] | None:
        """Coleta dados fiscais de produtos ou serviços presentes nesta nota fiscal,
        inclui também o valor do produto.

        - vund: Preço de cada unidade comercial *qcom* em
            `_TransacParser._get_product_amount()`;
        - bpis: Base de cálculo do 'PIS';
        - vpis: Valor recolhido do 'PIS';
        - bcofins: Base de cálculo do 'COFINS';
        - vcofins: Valor recolhido de 'COFINS';
        - bricms: Base de cálculo do 'ICMS' retido;
        - vricms: Valor recolhido do 'ICMS' retido;
        - vsicms: Valor recolhido do 'ICMS' por este vendedor;
        - bicms: Base de cálculo total prevista para o 'ICMS' deste produto;
        - vicms: Valor recolhido total prevista para o 'ICMS' deste produto.

        :param products: *Lista de bs4.BeautifulSoup*, cada uma com um segmento de
            dados de um produto.

        :return: *Lista de dicionários* contendo os dados informados acima, ou `None`
            caso não seja possível.
        :raises ParserParseError: Se não for possível obter todos os dados.
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
        """Função ajudante que coleta os dados relevantes para a tabela *fato*.

        :return: Um `FactRowElem` com os dados coletados, ou `None` caso não seja
            possível.
        :raises ParserParseError: Quando a coleta de dados não é possível
        """
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
        """Processa o documento e, caso tenha sucesso, vai popular `_TransacParser.data`,
        caso contrário, os erros serão registrados em `_TransacParser.err`.
        """
        if self._parsed():
            return
        rows = self._get_transac_rows()
        if rows is not None:
            self.data = self.data + rows


class FullParser(FactParser, _TransacParser):
    """*Parser* dedicado à extrair todos os dados do documento fornecido. Usado para
    preencher dados de tabelas *fato* e *transação* no banco de dados.

    Veja `BaseParser` e `_TransacParser` para mais informações.
    """

    def parse(self):
        """Processa o documento e, caso tenha sucesso, vai popular `FullParser.data`,
        caso contrário, os erros serão registrados em `FullParser.err`.
        """
        if self._parsed():
            return
        fact_rows = self._get_fact_rows()
        transac_rows = self._get_transac_rows()
        if fact_rows is not None:
            self.data = self.data + fact_rows
        if transac_rows is not None:
            self.data = self.data + transac_rows
