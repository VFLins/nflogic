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
    for i, n in enumerate(names):
        print(f"{i}, {n}")


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
        print(errdf)


@nflogic_cli.command()
def parse_dir(
    directory: Path,
    parse_to: Annotated[
        ParseTo,
        Option(
            help=(
                "Using 'both' option will create two pairs of tables in the the "
                "database, one to the seller and other to the buyer."
            ),
        ),
    ] = ParseTo.both,
    ignore_init_errors: Annotated[
        Optional[bool],
        Option("--ignore-init-errors/--parse-init-errors"),
    ] = True,
):
    """Parse all xml files in a directory."""
    if parse_to in ["buyer", "both"]:
        print("Parsing to buyer...")
        api.parse_dir(
            dir_path=directory,
            buy=True,
            full_parse=False,
            ignore_init_errors=ignore_init_errors,
        )
    if parse_to in ["seller", "both"]:
        print("Parsing to seller...")
        api.parse_dir(
            dir_path=directory,
            buy=False,
            full_parse=False,
            ignore_init_errors=ignore_init_errors,
        )


if __name__ == "__main__":
    nflogic_cli()
