from enum import Enum
from typer import Typer, Option, Argument
from nflogic import api
from pathlib import Path
from typing import Optional
from typing_extensions import Annotated

# doc_path = Path(__file__).parent.parent.parent / "docs" / "CLI.md"
# __doc__ = doc_path.read_text(encoding="utf-8") # use markdown file
# __all__ = [] # omit this files' contents from pdoc


class ParseTo(str, Enum):
    """@private Opções disponíveis para o argumento `--parse-to`."""

    buyer = "buyer"
    seller = "seller"
    both = "both"


nflogic_cli = Typer()
"""Aplicativo CLI onde os comandos são adicionados."""


@nflogic_cli.command(
    help="Mostra os nomes dos arquivos de cache produzidos pelo `nflogic`."
)
def cachenames():
    """`nflogic cachenames`
    ====

    Mostra os nomes dos arquivos de cache produzidos pelo `nflogic`.
    """
    names = api.cache.get_cachenames()
    n_dig = len(str(len(names)))
    for i, n in enumerate(names):
        print(f"{str(i).rjust(n_dig)} {n}")


@nflogic_cli.command(
    help=(
        "Lista os erros registrados em um arquivo de cache, use 'nflogic cachenames' "
        "para ver os nomes válidos."
    )
)
def errors(
    cachename: Annotated[str, Argument(help="Nome do arquivo de cache sem extensão.")],
    summary: Annotated[
        Optional[bool],
        Option(
            "--summary/--complete",
            help=(
                "Exibe os erros na forma detalhada (`--complete`) ou um resumo por "
                "tipo de erro (`--summary`)."
            ),
        ),
    ] = False,
):
    """`nflogic errors [OPÇÕES] CACHENAME`
    ====

    Lista os erros registrados em um arquivo de cache, use :func:`cachenames` para
    ver os nomes válidos.

    **`CACHENAME`** Nome do arquivo de cache sem extensão.

    ***Opções***
    - **`--summary / [--complete]`** Exibe os erros na forma detalhada por padrão, use
        `--summary` para ver um resumo por tipo de erro.
    """
    errdf = api.rebuild_errors(cachename)
    if summary:
        print(api.summary_err_types(errdf))
    else:
        print(errdf.to_string())


@nflogic_cli.command(help="Processa todos os arquivos XML de uma pasta.")
def parse(
    directory: Path,
    parse_to: Annotated[
        ParseTo,
        Option(
            help=(
                "Usar 'buyer' processa os arquivos como notas de compra, 'seller' "
                "como notas de venda, e o valor padrão 'both', para ambos os casos."
            ),
        ),
    ] = ParseTo.both,
    ignore_cached_errors: Annotated[
        Optional[bool],
        Option(
            "--ignore-cached-errors/--parse-cached-errors",
            help=(
                "Use `--parse-cached-errors` para tentar processar novamente os "
                "arquivos que já deram erro anteriormente."
            ),
        ),
    ] = True,
    full_parse: Annotated[
        Optional[bool],
        Option(
            "--full-parse/--partial-parse",
            help=(
                "`--full-parse` faz com que sejam processados tanto as informações de "
                "pagamento quanto os produtos no banco de dados, já `--partial-parse` "
                "registra apenas as informações de pagamento."
            ),
        ),
    ] = True,
):
    """`nflogic parse [OPÇÕES] DIRECTORY`
    ====

    Processa todos os arquivos XML de uma pasta.

    **`DIRECTORY`** Caminho para a pasta onde os arquivos XML estão armazenados.

    ***Opções***

    - **`--parse-to=buyer|seller|[both]`** Usar 'buyer' processa os arquivos como notas
        de compra, 'seller' como notas de venda, e o valor padrão, 'both', para ambos
        os casos.

    - **`[--ignore-cached-errors] / --parse-cached-errors`** O valor padrão vai ignorar
        arquivos que já foram processados sem sucesso, para tentar processar novamente
        estes arquivos, use a opção `--parse-cached-errors`.

    - **`[--full-parse] / --partial-parse`** O valor padrão faz com que sejam
        registrados tanto as informações de pagamento quanto os produtos no banco de
        dados, use `--partial-parse` para registrar apenas as informações de pagamento.

    .. note::
        Um processamento completo produz duas tabelas relacionadas no estilo
        *fato-transação*, enquanto que um processamento parcial produz apenas o
        equivalente à uma tabela *fato*.
    """
    if parse_to in ["buyer", "both"]:
        print("Parsing to buyer...")
        api.parse_dir(
            dir_path=directory,
            buy=True,
            full_parse=full_parse,
            ignore_cached_errors=ignore_cached_errors,
        )
    if parse_to in ["seller", "both"]:
        print("Parsing to seller...")
        api.parse_dir(
            dir_path=directory,
            buy=False,
            full_parse=full_parse,
            ignore_cached_errors=ignore_cached_errors,
        )


@nflogic_cli.command(help="Processa arquivos registrados em um cache de erros.")
def parse_cache(
    cachename: Annotated[str, Argument(help="Nome do arquivo de cache sem extensão.")],
):
    """`nflogic parse-cache CACHENAME`
    ====

    Processa arquivos registrados em um cache de erros.

    **`CACHENAME`** Nome do arquivo de cache sem extensão.
    """
    api.parse_cache(cachename=cachename, full_parse=True)


if __name__ == "__main__":
    nflogic_cli()
