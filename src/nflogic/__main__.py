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

    for inputs in c.data:
        p = parse.FactParser(inputs)
        p.parse()
        if p.erroed():
            err_types = [type(err) for err in p.err]
            err_msgs = [str(err) for err in p.err]
            new_row_errors_df = pd.DataFrame(
                [[p.INPUTS, err_types, err_msgs]], columns=df_columns
            )
            errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)

    return errors_df


def parse_on_dir(path: str, buy: bool, retry_failed: bool = False):
    """
    Tries to parse all xml files present in `path`.

    **Args**
        path: path to the directory that contains the xml files.
        buy: should all files be processed as buying notes? `False` if they sales notes.
        retry_failed: if a file has failed before, should we process it again? `False` if should skip all fails.
    """
    n_failed, n_add_to_cache, n_rm_from_cache, n_already_processed = 0, 0, 0, 0
    # TODO: Open a database connection at the beginning and close at the end of each run
    nfes = [
        os.path.join(path, filename)
        for filename in os.listdir(path)
        if ".xml" in filename.lower()
    ]

    n_files, n_iter = len(nfes), 1
    for file in nfes:
        print(f"This might take a while... {(n_iter/n_files)*100:.2f}%", end="\r")
        n_iter = n_iter + 1

        parser = parse.FactParser({"path": file, "buy": buy})
        fails_cache = cache.CacheHandler(parser.name)
        
        if not retry_failed and (parser.INPUTS in fails_cache.data):
            n_add_to_cache = n_add_to_cache + 1
            continue

        if parser._get_nfekey() in db.processed_keys(parser.name):
            n_already_processed = n_already_processed + 1
            continue

        parser.parse()
        if parser.erroed():
            n_failed = n_failed + 1
            if parser.INPUTS not in fails_cache.data:
                fails_cache.add(parser.INPUTS)
            continue

        try:
            db.insert_row(parser=parser, close=False)
            if retry_failed and (parser.INPUTS in fails_cache.data):
                fails_cache.rm(parser.INPUTS)
                n_rm_from_cache = n_rm_from_cache + 1
        except Exception as err:
            print(str(err))
            # TODO: add exception management
            fails_cache.add(parser.INPUTS)
    
    msgs = [
        f"{n_files} xml files in {path}",
        f"{n_add_to_cache} failed",
        f"{n_already_processed} were already in the database"
    ]
    if retry_failed:
        msgs.append(f"{n_rm_from_cache} failed before, but are now in the database")
    print(*msgs, sep="\n")


if __name__ == "__main__":
    pass
