# Grand Comics Database™ plugin for Comic Tagger

A plugin for [Comic Tagger](https://github.com/comictagger/comictagger/releases) to allow the use of the metadata from [Grand Comics Database™](https://www.comics.org).

## Work in progress

Pre-alpha - Proof of concept

Requires a version of Comic Tagger @develop

Requires a SQLite version of the MySQL [dump](https://www.comics.org/download/) from GCD. An account is required to download the dump file.

### Conversion options

Use the AWK script [mysql2sqlite](https://github.com/dumblob/mysql2sqlite).

Restore the dump to MySQL/MariaDB and use the [MySQL to SQLite3](https://github.com/techouse/mysql-to-sqlite3) `pip install mysql-to-sqlite3` tool.

*Be warned the size will double to around 4GB.*

You're welcome to try other conversion methods but your milage may vary.

### Cover images

An option is available to attempt to download the covers in the GUI and separately for auto-tagging.
Due to occasional CloudFlare activation, images may not download.

## Install

`pip install .`
