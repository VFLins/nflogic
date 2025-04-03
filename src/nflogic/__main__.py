import os
import pandas as pd
from nflogic import db, cache, parse


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

    **Args**
        cachename: name of the cache to retrieve errors from.

    **Returns**
        `pandas.DataFrame` with column names "Inputs", "ErrorType" and "ErrorMessage"

    **Raises**
        `KeyError` if `cachename` doesn't exist, use `nflogic.cache.get_cachenames()` to check available cache names.
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

    **Args**
        errdf: Pandas dataframe where:
            - `errdf.columns == ["Inputs", "ErrorType", "ErrorMessage"]`
            - `errdf.dtypes == [dict, list, list]`

    **Returns**
        `pandas.DataFrame`
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


def _handle_parser_errors(parser_input: parse.ParserInput, full_parse: bool) -> parse.FactParser | None:
    if full_parse:
        #parser = parse.FullParser(parser_input)
        raise NotImplementedError("Not able to perform full parsing yet.")
    else:
        parser = parse.FactParser(parser_input)

    if parser.erroed():
        n_failed = n_failed + 1
        cache._save_failed_parser_init(parser.INPUTS)
        return

    parser.parse()
    if parser.erroed():
        n_failed = n_failed + 1
        cache_handler = cache.CacheHandler(parser.name)
        if parser.INPUTS not in cache_handler.data:
            cache_handler.add(parser.INPUTS)
        return
    return parser

def parse_on_dir(
    dir_path: str, buy: bool, full_parse: bool = False, ignore_init_errors: bool = True
):
    """
    Tries to parse all xml files present in `path`.

    Args
        dir_path: path to the directory that contains the xml files
        buy: should all files be processed as buying notes? `False` if they are
          sales notes
        ignore_init_errors: wether to ignore files that could not be parsed by
          `xmltodict` before or not
    """
    # TODO: Open a database connection at the beginning and close at the end of each run
    nfes = xml_files_in_dir(dir_path=dir_path)
    new_parser_inputs = cache.get_not_processed_inputs(
        filepaths=nfes, buy=buy, ignore_not_parsed=ignore_init_errors
    )
    n_iter, n_failed, n_recovered = 0, 0, 0
    try:
        for parser_input in new_parser_inputs:
            n_iter = n_iter + 1
            print(f"This might take a while... {n_iter} files processed.", end="\r")

            parser = _handle_parser_errors(parser_input, full_parse=full_parse)
            if parser is None:
                n_failed = n_failed + 1
                continue

            if parser.data.ChaveNFe in db.processed_keys(parser.name):
                # TODO: create a function to test this conditional inside the database
                # instead of retrieving rows from database and checking in python
                cache._save_successfull_fileparse(parser_input=parser_input)
                continue

            db.insert_row(parser=parser, close=False)
            cache._save_successfull_fileparse(parser_input=parser_input)
            fails_cache = cache.CacheHandler(parser.name)
            if parser_input in fails_cache.data:
                fails_cache.rm(parser_input)
                n_recovered = n_recovered + 1
    except KeyboardInterrupt:
        pass

    msgs = [f"{n_iter} xml files processed in {dir_path}"]
    if n_iter > 0:
        msgs = msgs + [f"{n_failed} failed"]
    print(*msgs, sep="\n")


def parse_on_cache(cachename: str):
    fails_cache = cache.CacheHandler(cachename)
    n_iter, n_failed, n_skipped, n_recovered = 0, 0, 0, 0
    try:
        for parser_input in fails_cache.data:
            n_iter = n_iter + 1
            print(f"This might take a while... {n_iter} files processed.", end="\r")

            parser = parse.FactParser(parser_input)
            if parser.erroed():
                n_failed = n_failed + 1
                cache._save_failed_parser_init(parser_input)
                continue

            parser.parse()
            if parser.erroed():
                n_failed = n_failed + 1
                continue

            if parser._get_nfekey() in db.processed_keys(parser.name):
                # TODO: create a function to test this conditional inside the database
                # instead of retrieving rows from database and checking in python
                cache._save_successfull_fileparse(parser_input=parser_input)
                fails_cache.rm(parser_input)
                n_skipped = n_skipped + 1
                continue

            db.insert_row(parser=parser, close=False)
            cache._save_successfull_fileparse(parser_input=parser_input)
            fails_cache.rm(parser_input)
            n_recovered = n_recovered + 1

    except KeyboardInterrupt:
        pass

    msgs = [f"{n_iter} xml files processed from {cachename}.cache"]
    if n_iter > 0:
        msgs = msgs + [
            f"{n_failed} could not be recovered",
            f"{n_recovered} removed from cache, and are now in the database",
            f"{n_skipped} removed from cache, and were already in the database",
        ]
    print(*msgs, sep="\n")


if __name__ == "__main__":
    pass
