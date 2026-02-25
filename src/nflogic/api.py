import os
import sqlite3
import pandas as pd
from nflogic import cache, parse

# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
CACHE_DIR = os.path.join(SCRIPT_DIR, "cache")
DB_PATH = os.path.join(SCRIPT_DIR, "data", "main.sqlite")


# FEATURES
###############


def xml_files_in_dir(dir_path: str):
    """Return full path of every file with .xml extension in `dir_path`."""
    return [
        os.path.join(dir_path, filename)
        for filename in os.listdir(dir_path)
        if os.path.splitext(filename)[1] == ".xml"
    ]


def rebuild_errors(cachename: str) -> pd.DataFrame:
    """
    Creates a data frame with all the errors rebuilt from the given cache.

    :param cachename: name of the cache to retrieve errors from.
    :return: `pandas.DataFrame` with column names "Inputs", "ErrorType" and
      "ErrorMessage"
    :raise: `KeyError` if `cachename` doesn't exist, use
      `nflogic.cache.get_cachenames()` to check available cache names.
    """
    if cachename not in cache.get_cachenames():
        raise KeyError("Not valid cachename.")

    df_columns = ["Inputs", "ErrorType", "ErrorMessage"]
    errors_df = pd.DataFrame(columns=df_columns)
    c = cache.CacheHandler(cachename)

    def new_row_err(parser_err, parser_inputs):
        err_types = [type(err) for err in parser_err]
        err_msgs = [str(err) for err in parser_err]
        row_data = [parser_inputs, err_types, err_msgs]
        return pd.DataFrame([row_data], columns=df_columns)

    for inputs in c.data:
        # capture init error
        p = parse.FactParser(inputs)
        if p.erroed():
            new_row_errors_df = new_row_err(p.err, p.INPUTS)
            errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)
            continue
        # capture parse/validation error
        p.parse()
        if p.erroed():
            new_row_errors_df = new_row_err(p.err, p.INPUTS)
            errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)
    return errors_df


def summary_err_types(errdf: pd.DataFrame):
    """
    Returns a summary of error types for a dataframe returned by `rebuild_errors()`.

    :param errdf: Pandas dataframe where:
        - `errdf.columns == ["Inputs", "ErrorType", "ErrorMessage"]`
        - `errdf.dtypes == [dict, list, list]`
    :return: `pandas.DataFrame`
    """
    errdf["InitFail"] = tuple(
        map(lambda x: parse.ParserInitError in x, errdf["ErrorType"])
    )
    errdf["ParseFail"] = tuple(
        map(lambda x: parse.ParserParseError in x, errdf["ErrorType"])
    )
    errdf["ValidationFail"] = tuple(
        map(lambda x: parse.ParserValidationError in x, errdf["ErrorType"])
    )
    summary = errdf.groupby(["InitFail", "ParseFail", "ValidationFail"])[
        ["InitFail"]
    ].count()
    summary.columns = ["Count"]
    return summary


def parse_dir(
    dir_path: str,
    buy: bool,
    full_parse: bool = False,
    ignore_cached_errors: bool = True,
    con: sqlite3.Connection = cache.db.sqlite3.connect(cache.db.DB_PATH),
):
    """
    Tries to parse all xml files present in `path`.

    :param dir_path: path to the directory that contains the xml files
    :param buy: should all files be processed as buying notes? `False` if they are
      sales notes
    :param ignore_init_errors: wether to ignore files that could not be parsed by
      `BeautifulSoup` before or not
    :con: A `sqlite3.Connection` object, indicating which database to connect.
    """
    # TODO: Open a database connection at the beginning and close at the end of each run
    try:
        nfes = xml_files_in_dir(dir_path=dir_path)
        new_parser_inputs = cache.get_not_processed_inputs(
            filepaths=nfes,
            buy=buy,
            ignore_fails=ignore_cached_errors,
            full_parse=full_parse,
        )
        man = cache.ParserManipulator(full_parse=full_parse, con=con)
        for parser_input in new_parser_inputs:
            man.add_parser(parser_input)
            print(
                f"This might take a while... {man.n_parsed} files processed.", end="\r"
            )
    except KeyboardInterrupt:
        pass
    msgs = [f"{man.n_parsed} xml files processed in {dir_path}"]
    if man.n_parsed > 0:
        msgs = msgs + [
            f"{man.n_failed} failed",
            f"{man.n_skipped} already in the database completely or partially",
        ]
    print(*msgs, sep="\n")


def parse_cache(
    cachename: str,
    full_parse: bool = False,
    con: sqlite3.Connection = cache.db.sqlite3.connect(cache.db.DB_PATH),
):
    """Tries to parse all documents listed in a cache file.

    :param cachename: Name of the cache file.
    :param full_parse: If `True`, process data for both fact and transaction tables,
      otherwise will process data only for fact table.
    :con: A `sqlite3.Connection` object, indicating which database to connect.
    """
    try:
        fails_cache = cache.CacheHandler(cachename, full_parse)
        man = cache.ParserManipulator(full_parse, con=con)
        for parser_input in fails_cache.data:
            man.add_parser(parser_input)
            print(
                f"This might take a while... {man.n_parsed} files processed.", end="\r"
            )
    except KeyboardInterrupt:
        pass
    msgs = [f"{man.n_parsed} xml files processed from {cachename}.cache"]
    if man.n_parsed > 0:
        msgs = msgs + [
            f"{man.n_failed} could not be recovered",
            f"{man.n_recovered} removed from cache, and are now in the database",
            f"{man.n_skipped} removed from cache, and were already in the database",
        ]
    print(*msgs, sep="\n")


if __name__ == "__main__":
    pass
