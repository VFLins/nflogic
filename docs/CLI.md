# `nflogic`

**Usage**:

```console
$ nflogic [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--install-completion`: Install completion for the current shell.
* `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
* `--help`: Show this message and exit.

**Commands**:

* `cachenames`: Display cache names of cachefiles produced...
* `errors`: List errors stored in a cache file, see...
* `parse`: Parse all xml files in a directory.
* `parse-cache`: Parse data from cache file.

## `nflogic cachenames`

Display cache names of cachefiles produced by nflogic.

**Usage**:

```console
$ nflogic cachenames [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `nflogic errors`

List errors stored in a cache file, see `nflogic cachenames` to see available
file names.

**Usage**:

```console
$ nflogic errors [OPTIONS] CACHENAME
```

**Arguments**:

* `CACHENAME`: [required]

**Options**:

* `--summary / --complete`: [default: complete]
* `--help`: Show this message and exit.

## `nflogic parse`

Parse all xml files in a directory.

**Usage**:

```console
$ nflogic parse [OPTIONS] DIRECTORY
```

**Arguments**:

* `DIRECTORY`: [required]

**Options**:

* `--parse-to [buyer|seller|both]`: Using &#x27;both&#x27; option will create tables both the seller and the buyer in the database.  [default: both]
* `--ignore-cached-errors / --parse-cached-errors`: [default: ignore-cached-errors]
* `--full-parse / --partial-parse`: A full parse will produce a pair of tables following a fact/transaction standard, while a partial parse will only produce the equivalent to the fact table.  [default: full-parse]
* `--help`: Show this message and exit.

## `nflogic parse-cache`

Parse data from cache file.

**Usage**:

```console
$ nflogic parse-cache [OPTIONS] CACHENAME
```

**Arguments**:

* `CACHENAME`: [required]

**Options**:

* `--help`: Show this message and exit.
