import gzip
import json
import re
import sys
import urllib.request
from collections import namedtuple
import lightrdf
from pydgraph import DgraphClient, Txn, DgraphClientStub
from rdflib import RDFS, URIRef, RDF, Literal

Lnk = namedtuple('Lnk', ('parent', 'child'))

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


class BaseSink:
    def __init__(self, client: DgraphClient):
        self._client = client
        self._txn = None

    def get_txn(self) -> Txn:
        if self._txn is None:
            self._txn = self._client.txn()
        return self._txn

    def discard(self):
        if self._txn is not None:
            self._txn.discard()

    def commit(self):
        if self._txn is not None:
            self._txn.commit()
            self._txn = None

    def to_n3(self, s):
        if s[0] != '"':
            return URIRef(s)
        m = re.search(r'^"(.*)"\^\^<(.*)>$', s)
        if m:
            type = m.group(2)
            lit = m.group(1)
            return Literal(lit, datatype=type)
        else:
            return Literal(s[1:-1])

    def collect(self, s, p, o):
        s = self.to_n3(s)
        p = self.to_n3(p)
        o = self.to_n3(o)

        if str(s).endswith("/wiki/Special:CategoryDump"):
            return

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

    def close(self):
        pass


class NodeImport(BaseSink):
    def __init__(self, client: DgraphClient):
        BaseSink.__init__(self, client)
        self._chunk = []
        self._size = 0

    def close(self):
        if len(self._chunk) > 0:
            self.add_nodes()
            self.commit()

    def node(self, id: URIRef, hidden: bool):
        self._chunk.append((id.toPython(), hidden))
        self._size += 1
        if len(self._chunk) > 1000:
            self.add_nodes()

    def add_nodes(self):
        self.get_txn().mutate(set_obj=[flatten_simple_node(u) for u in self._chunk])
        if (self._size % 10000) == 0:
            self.commit()
        self._chunk = []


class PredsImport(BaseSink):
    def __init__(self, client: DgraphClient):
        BaseSink.__init__(self, client)
        self._chunk = []
        self._size = 0

    def pred(self, id: URIRef, pred: str, val):
        self._chunk.append((id, pred, val))
        if len(self._chunk) > 1000:
            self._size += len(self._chunk)
            self.add_preds()

    def add_preds(self):
        uid_lookups = set(str(s) for (s, p, o) in self._chunk)
        uid_lookups |= set(str(o) for (s, p, o) in self._chunk if isinstance(o, URIRef))
        uid_lookups = {name: f"v{idx}" for idx, name in enumerate(uid_lookups)}

        uid_vars = "{\n"
        for cat, var_name in uid_lookups.items():
            cat = str(cat).replace("\\", "\\\\").replace("\"", "\\\"")
            uid_vars += f"  {var_name} as var(func: eq(pageUrl, \"{cat}\"))\n"
        uid_vars += "}\n"
        triples = ""
        for (s, p, o) in self._chunk:
            child_var = uid_lookups[str(s)]
            if isinstance(o, URIRef) and p == "parentCategories":
                parent_var = uid_lookups[str(o)]
                triples += f"uid({child_var}) <parentCategories> uid({parent_var}) .\n"
            elif not isinstance(o, URIRef):
                triples += f"uid({child_var}) <{p}> {o.n3()}. \n"
            else:
                raise ValueError(f"Unexpected triple: {(s, p, o)}")

        txn = self.get_txn()
        mut = txn.create_mutation(set_nquads=triples)
        req = txn.create_request(query=uid_vars, mutations=[mut])
        txn.do_request(req)
        if (self._size % 10000) == 0:
            self.commit()
        self._chunk = []

    def close(self):
        if len(self._chunk) > 0:
            self.add_preds()
            self.commit()


def import_rdf(input_file: str, client: DgraphClient):
    rdf_parser = lightrdf.turtle.Parser()

    def collect(file, importer: BaseSink):
        try:
            for s, p, o in rdf_parser.parse(file):
                importer.collect(s, p, o)
            importer.close()
        finally:
            importer.discard()

    with gzip.open(urllib.request.urlopen(input_file)) as file:
        collect(file, NodeImport(client))

    with gzip.open(urllib.request.urlopen(input_file)) as file:
        collect(file, PredsImport(client))


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


dgraph_client = DgraphClientStub('localhost:9080')
dgraph_client = DgraphClient(dgraph_client)

input_file = sys.argv[1]

if input_file.endswith(".json.gz"):
    import_json(input_file, dgraph_client)
elif input_file.endswith("ttl.gz"):
    import_rdf(input_file, dgraph_client)

