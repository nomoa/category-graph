# Utilities to manipulate categories

Setup a python venv:

```shell
python3 -m venv ~/.virtualenvs/category-graph/
source  ~/.virtualenvs/category-graph/bin/source
pip install -r requirements.txt
```

## convert_rdf_to_json.py

Converts a category RDF dump into a `ndjson` dump.

```shell
python convert_rdf_to_json.py https://dumps.wikimedia.your.org/other/categoriesrdf/20211002/mediawikiwiki-20211002-categories.ttl.gz | gzip -c > mediawikiwiki-20211002-categories.json.gz
```
