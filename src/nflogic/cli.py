from typer import Typer
from nflogic import api


nflogic_cli = Typer()

@nflogic_cli.command()
def cachenames():
    """Display cache names of cachefiles produced by nflogic."""
    names = api.cache.get_cachenames()
    for i, n in enumerate(names):
        print(f"{i}, {n}")


@nflogic_cli.command()
def errors(cachename: str, summary: bool = False):
    errdf = api.rebuild_errors(cachename)
    if summary:
        print(api.summary_err_types(errdf))
    else:
        print(errdf)

if __name__ == "__main__":
    nflogic_cli()

