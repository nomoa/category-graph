import gzip
import json
import sys
import urllib.request

from rdflib import Graph, URIRef, RDFS, RDF

# Converts a RDF dump into a ndjson file (one json doc per line)
#
# Usage:
# convert_rdf_to_json.py https://dumps.wikimedia.your.org/other/categoriesrdf/20211002/mediawikiwiki-20211002-categories.ttl.gz | gzip -c > mediawikiwiki-20211002-categories.json.gz
# Requires:
# - python 3
# - pip install rdflib


LABEL = RDFS.label
NPAGES = URIRef("https://www.mediawiki.org/ontology#pages")
NSUBCATEGS = URIRef("https://www.mediawiki.org/ontology#subcategories")
IS_IN_CATEG = URIRef("https://www.mediawiki.org/ontology#isInCategory")
CATEG_TYPE = URIRef("https://www.mediawiki.org/ontology#Category")
HIDDEN_CATEG_TYPE = URIRef("https://www.mediawiki.org/ontology#HiddenCategory")

RDF_VOCAB_MAP = {
    LABEL: "name",
    NPAGES: "numberOfPages",
    NSUBCATEGS: "numberOfCategories",
}

g = Graph()
input = sys.argv[1]
with gzip.open(urllib.request.urlopen(input)) as file:
    g.load(source=file, format="ttl")
categories = set(g.subjects())

for c in categories:
    if (str(c).endswith("/wiki/Special:CategoryDump")):
        # skip dump metadata
        continue
    categData = {
        "id": str(c),
        "pageUrl": str(c),
        "parentCategories": []
    }
    for (s, p, o) in g.triples([c, None, None]):
        if p == RDF.type:
            if o == CATEG_TYPE:
                categData["hidden"] = False
            elif o == HIDDEN_CATEG_TYPE:
                categData["hidden"] = True
            else:
                raise ValueError(f"Invalid type {o}")
            continue

        if p == IS_IN_CATEG:
            categData['parentCategories'].append(str(o))
            continue

        k = RDF_VOCAB_MAP.get(p)
        if k is None:
            raise ValueError(f"Unknown predicate {p}")
        categData[k] = str(o)
    json.dump(categData, sys.stdout)
    sys.stdout.write("\n")
