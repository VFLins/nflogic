from enum import Enum
from typer import Typer, Option
from nflogic import api
from pathlib import Path
from typing import Optional
from typing_extensions import Annotated


class ParseTo(str, Enum):
    buyer = "buyer"
    seller = "seller"
    both = "both"


nflogic_cli = Typer()


@nflogic_cli.command()
def cachenames():
    """Display cache names of cachefiles produced by nflogic."""
    names = api.cache.get_cachenames()
    n_dig = len(str(len(names)))
    for i, n in enumerate(names):
        print(f"{str(i).rjust(n_dig)} {n}")


@nflogic_cli.command()
def errors(
    cachename: str,
    summary: Annotated[Optional[bool], Option("--summary/--complete")] = False,
):
    """List errors stored in a cache file, see `nflogic cachenames` to see available
    file names.
    """
    errdf = api.rebuild_errors(cachename)
    if summary:
        print(api.summary_err_types(errdf))
    else:
        print(errdf.to_string())


@nflogic_cli.command()
def parse(
    directory: Path,
    parse_to: Annotated[
        ParseTo,
        Option(
            help=(
                "Using 'both' option will create tables both the seller and "
                "the buyer in the database."
            ),
        ),
    ] = ParseTo.both,
    ignore_cached_errors: Annotated[
        Optional[bool],
        Option("--ignore-cached-errors/--parse-cached-errors"),
    ] = True,
    full_parse: Annotated[
        Optional[bool],
        Option(
            "--full-parse/--partial-parse",
            help=(
                "A full parse will produce a pair of tables following a "
                "fact/transaction standard, while a partial parse will only "
                "produce the equivalent to the fact table."
            ),
        )
    ] = True
):
    """Parse all xml files in a directory."""
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


@nflogic_cli.command()
def parse_cache(cachename: str):
    """Parse data from cache file."""
    api.parse_cache(cachename=cachename, full_parse=True)


if __name__ == "__main__":
    nflogic_cli()
