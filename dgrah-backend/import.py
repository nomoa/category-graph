import gzip
import json
import sys
import urllib.request
from collections import namedtuple
from parser import ParserError

import rdflib.parser
from rdflib import parser, RDFS, URIRef, RDF

from pydgraph import DgraphClient, Txn, DgraphClientStub
from rdflib.plugins.parsers.notation3 import RDFSink

LABEL = RDFS.label
NPAGES = URIRef("https://www.mediawiki.org/ontology#pages")
NSUBCATEGS = URIRef("https://www.mediawiki.org/ontology#subcategories")
IS_IN_CATEG = URIRef("https://www.mediawiki.org/ontology#isInCategory")
CATEG_TYPE = URIRef("https://www.mediawiki.org/ontology#Category")
HIDDEN_CATEG_TYPE = URIRef("https://www.mediawiki.org/ontology#HiddenCategory")

def flatten_for_dgraph(d: dict) -> dict:
    return {
        "name": d["name"],
        "pageUrl": d["pageUrl"],
        "numberOfPages": d["numberOfPages"],
        "numberOfCategories": d["numberOfCategories"],
        "dgraph.type": "Category"
    }

def flatten_simple_node(categ: tuple[str, bool]) -> dict:
    return {
        "pageUrl": categ[0],
        "hidden": categ[1],
        "dgraph.type": "Category"
    }


def dgraph_import_doc(docs: list[dict], txn: Txn):
    txn.mutate(set_obj=list(map(flatten_for_dgraph, docs)))


Lnk = namedtuple('Lnk', ('parent', 'child'))


def flatten_links(categDoc: dict):
    for p in categDoc["parentCategories"]:
        yield Lnk(parent=p, child=categDoc['pageUrl'])


def dgraph_import_links(links: list[Lnk], txn: Txn):
    uid_lookups = set(c for d in links for c in [d.parent, d.child])
    uid_lookups = {name: f"v{idx}" for idx, name in enumerate(uid_lookups)}
    data = "{\n"
    for cat, var_name in uid_lookups.items():
        cat = str(cat).replace("\\", "\\\\").replace("\"", "\\\"")
        data += f"  {var_name} as var(func: eq(pageUrl, \"{cat}\"))\n"
    data += "}\n"
    triples = ""
    for link in links:
        child_var = uid_lookups[link.child]
        parent_var = uid_lookups[link.parent]
        triples += f"uid({child_var}) <parentCategories> uid({parent_var}) .\n"

    mut = txn.create_mutation(set_nquads=triples)
    request = txn.create_request(query=data, mutations=[mut])
    txn.do_request(request)


client = DgraphClientStub('localhost:9080')
client = DgraphClient(client)

input_file = sys.argv[1]


class BaseSink(RDFSink):
    def __init__(self, client: DgraphClient):
        RDFSink.__init__(graph=rdflib.Graph())
        self._client = client
        self._txn = None

    def get_txn(self) -> Txn:
        if self._txn is None:
            self._txn = client.txn()

    def discard(self):
        if self._txn is not None:
            self._txn.discard()

    def commit(self):
        if self._txn is not None:
            self._txn.commit()
            self._txn = None
        else:
            raise ValueError("No active transaction")

    def makeStatement(self, quadruple, why=None):
        f, p, s, o = quadruple

        if hasattr(p, "formula"):
            raise ParserError("Formula used as predicate")

        elif f != self.rootFormula:
            raise ValueError("root only supported")

        s = self.normalise(f, s)
        p = self.normalise(f, p)
        o = self.normalise(f, o)

        if p == RDF.type:
            if o == CATEG_TYPE:
                self.node(s, False)
            elif o == HIDDEN_CATEG_TYPE:
                self.node(s, True)
            else:
                raise ValueError(f"Invalid type {o}")

            return

        elif p == IS_IN_CATEG:
            self.pred(s, "parentCategories", o)
        elif p == LABEL:
            self.pred(s, "name", o)
        elif p == NPAGES:
            self.pred(s, "numberOfPages", o)
        elif p == NSUBCATEGS:
            self.pred(s, "numberOfCategories", o)

    def node(self, id: URIRef, hidden: bool):
        pass

    def pred(self, id: URIRef, pred: str, val):
        pass


class NodeImport(BaseSink):
    def __init__(self, client: DgraphClient):
        BaseSink.__init__(self, client)
        self._chunk = []

    def node(self, id: URIRef, hidden: bool):
        self._chunk.append((id.toPython(), hidden))
        if len(self._chunk) > 1000:
            self.add_nodes()

    def add_nodes(self):
        self.get_txn().mutate(set_obj=[flatten_simple_node(u) for u in self._chunk])
        self._chunk = []

class PredsImport(BaseSink):

    def __init__(self, client: DgraphClient):
        BaseSink.__init__(self, client)
        self._chunk = []
        self._size = 0

    def pred(self, id: URIRef, pred: str, val):
        self._chunk.index((id, pred, val))
        if len(self._chunk) > 1000:
            self._size += len(self._chunk)
            self.add_preds()


    def add_preds(self):
        uid_lookups = set(str(s) for (s, p, o) in self._chunk)
        uid_lookups |= set(str(o) for (s, p, o) in self._chunk if o.isinstance(URIRef))
        uid_lookups = {name: f"v{idx}" for idx, name in enumerate(uid_lookups)}

        data = "{\n"
        for cat, var_name in uid_lookups.items():
            cat = str(cat).replace("\\", "\\\\").replace("\"", "\\\"")
            data += f"  {var_name} as var(func: eq(pageUrl, \"{cat}\"))\n"
        data += "}\n"
        triples = ""
        for (s, p, o) in self._chunk:
            if s.isintance(URIRef):
                # here
            child_var = uid_lookups[link.child]
            parent_var = uid_lookups[link.parent]
            triples += f"uid({child_var}) <parentCategories> uid({parent_var}) .\n"

        mut = txn.create_mutation(set_nquads=triples)
        request = txn.create_request(query=data, mutations=[mut])
        self.get_txn().mutate(set_obj=[flatten_simple_node(u) for u in self._chunk])
        self._chunk = []


def import_rdf(input_file: str, client):
    parser = rdflib.plugin.get("ttl", rdflib.parser.Parser)

    with gzip.open(urllib.request.urlopen(input_file)) as file:
        parser.parse()


def import_json(input_file: str, client):
    with gzip.open(urllib.request.urlopen(input_file)) as file:
        chunk = []
        try:
            txn = client.txn()
            for line in file:
                doc = json.loads(line)
                chunk.append(doc)
                if len(chunk) > 1000:
                    dgraph_import_doc(chunk, txn)
                    chunk = []
            if len(chunk) > 0:
                dgraph_import_doc(chunk, txn)
            txn.commit()
        finally:
            txn.discard()

    # 2nd pass
    with gzip.open(urllib.request.urlopen(input_file)) as file:
        links = []
        nb_links = 0
        try:
            txn = client.txn()
            for line in file:
                doc = json.loads(line)
                for link in flatten_links(doc):
                    links.append(link)
                    nb_links = nb_links + len(links)
                    if len(links) > 1000:
                        dgraph_import_links(links, txn)
                        links = []
                    if nb_links % 10000:
                        # the client holds a state https://dgraph.io/docs/clients/raw-http/
                        # and it can't grow indefinitely
                        txn.commit()
                        txn = client.txn()
            if len(links) > 0:
                dgraph_import_links(links, txn)
            txn.commit()
        finally:
            txn.discard()
