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


def parse_on_dir(path: str, buy: bool):
    """
    Tries to parse all xml files present in `path`.

    **Args**
        path: path to the directory that contains the xml files.
        buy: should all files be processed as buying notes? `False` if they sales notes.
        retry_failed: if a file has failed before, should we try to parse it again? `False` if should skip all fails.
    """
    n_failed, n_add_to_cache, n_rm_from_cache, n_already_processed = 0, 0, 0, 0
    # TODO: Open a database connection at the beginning and close at the end of each run
    nfes = [
        os.path.join(path, filename)
        for filename in os.listdir(path)
        if ".xml" in filename.lower()
    ]
    new_parser_inputs = cache.get_not_processed_inputs(filepaths=nfes, buy=buy)

    n_iter = 1
    for parser_input in new_parser_inputs:
        print(f"This might take a while... {n_iter} files processed.", end="\r")
        n_iter = n_iter + 1

        parser = parse.FactParser(parser_input)
        fails_cache = cache.CacheHandler(parser.name)

        if parser._get_nfekey() in db.processed_keys(parser.name):
            # TODO: create a function to test this conditional inside the database
            # instead of retrieving rows from database and checking in python
            cache.save_successfull_fileparse(parser_input=parser_input)
            n_already_processed = n_already_processed + 1
            continue

        parser.parse()
        if parser.erroed():
            n_failed = n_failed + 1
            if parser_input not in fails_cache.data:
                fails_cache.add(parser_input)
            continue

        db.insert_row(parser=parser, close=False)
        cache.save_successfull_fileparse(parser_input=parser_input)
        if parser_input in fails_cache.data:
            fails_cache.rm(parser_input)
            n_rm_from_cache = n_rm_from_cache + 1

    msgs = [
        f"{n_files} new xml files in {path}",
        f"{n_add_to_cache} failed",
        f"{n_rm_from_cache} failed before, but are now in the database"
    ]
    print(*msgs, sep="\n")


if __name__ == "__main__":
    pass
