# Grand Comics Database™ plugin for Comic Tagger

A plugin for [Comic Tagger](https://github.com/comictagger/comictagger/releases) to allow the use of the metadata from [Grand Comics Database™](https://www.comics.org).

## Obtaining the SQLite DB

1. Create an account on the [GCD](https://www.comics.org).
2. Download the latest SQLite3 dump of their DB (minus images and image URLs) at https://www.comics.org/download/

### Cover images

**GCD does not make their image URLs available via their DB dumps**

An option is available to attempt to download the covers in the GUI and separately for auto-tagging.
Due to occasional CloudFlare activation, images may not download.

## Install

`pip install .`
