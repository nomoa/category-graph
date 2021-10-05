# Category graph serving layer prototyping

Set of experiments related to put in place a service that replaces the current SPARQL endpoint powered by the WDQS blazegraph cluster.
Context: https://phabricator.wikimedia.org/T292404

## Serving layer

Prototypes of graphdb able to fulfill the needs of the categories SPARQL endpoint:
- Breadth first search over the category tree
- Simple point queries to get the object properties

### Property Graph backends

Prototype expectations:
- load the data from the json dumps
- able to mutate the graph from a json event (for real-time updates)
- ideally have a WMCS service with the backend running

Please add any PG graph engine you would like to experiment with

#### dgraph

In folder [dgraph-backend](dgraph-backend)

#### orientdb

In folder [orientdb-backend](orientdb-backend)

### RDF graph DBs

#### Apache Fuzeki & RDF-HDT

In folder [rdf-fuzeki-hdt-backend](rdf-fuzeki-hdt-backend)

## Data pipeline

### Batch

Using spark in [spark-batch-loader](spark-batch-loader).

Goals:
- Get the RDF dumps into HDFS possibly using bz2 to ease splitting (dumps source: `/mnt/data/xmldatadumps/public/other/categoriesrdf/`)
- Create a hive table with context, subject, predicate, object and store similar to the wikibase approach
- Convert the dumps to an HDT file, might require https://github.com/rdfhdt/hdt-mr

Daily process (nice to have):
- process daily sparql dumps and update the date fetched above
- Create a new HDT file

### Real-time

Using flink see: TODO add link to project

### Testing resources

## RDF dumps

https://dumps.wikimedia.your.org/other/categoriesrdf/

## HDT files

If you want to experiment with jena-fuzeki & HDT without having to build your HDT file:
- https://people.wikimedia.org/~dcausse/frwiki-20210918-categories.hdt

## Json Dumps

- mediawikiwiki: https://people.wikimedia.org/~dcausse/mediawikiwiki-20211002-categories.json.gz

Please ask if you prefer a bigger/smaller example or convert one yourself using `tools/convert_rdf_to_json.py`.

## Nice tools to have

See [tools](tools)

- convert_rdf_to_json.py: A tool to convert RDF dumps into json dumps (ndjson: one doc per line)

## Formats and schema

Category document structure:
```graphql
type Category {
    """ ID for the category page. (The RDF model conflates the page_url and the ID should we do the same here?) """
    id: ID!

    """ Name of the page """
    name: String!

    """ URL of the category page """
    pageUrl: String!
    
    """ category visibility (categories generally not displayed at the end of the page)"""
    hidden: Boolean!

    """ Categories this category belongs to """
    parentCategories: [Category!]!

    """ Number of pages belonging to this category (direct relationships) """
    numberOfPages: Int!

    """ Number of categories belonging to this category (direct relationships) """
    numberOfCategories: Int!
}
```

Example json doc for dumps:

```json
{
    "id": "https://commons.wikimedia.org/wiki/Category:Trees",
    "name": "Trees",
    "pageUrl": "https://commons.wikimedia.org/wiki/Category:Trees",
    "hidden": false,
    "parentCategories": [
        "https://commons.wikimedia.org/wiki/Category:Plants_by_common_named_groups",
        "https://commons.wikimedia.org/wiki/Category:Woody_plants",
        "https://commons.wikimedia.org/wiki/Category:Plant_life-form"
    ],
    "numberOfPages": 13,
    "numberOfCategories": 83
}
```

(Coordinate with linkstable hackathon project).

Example mutation events (for testing, the flink pipeline might perhaps perform the updates itself using backend specific update-DSL):

Removing a parent category (e.g. **Woody_plants** is no longer parent of **Trees**):
- must remove **Woody_plants** from the `parentCategories` array of **Trees**
- must decrement the `numberOfCategories` of **Woody_plants** by 1

```json
{
    "id": "https://commons.wikimedia.org/wiki/Category:Trees",
    "removedParentCategories": [
        "https://commons.wikimedia.org/wiki/Category:Woody_plants"
    ]
}
```

```json
{
    "id": "https://commons.wikimedia.org/wiki/Category:Woody_plants",
    "numberOfCategories": 4
}
```

Adding a parent category (e.g. **Woody_plants** is re-added as a parent of Trees):

```json
{
  "id": "https://commons.wikimedia.org/wiki/Category:Trees",
  "addedParentCategories": [
    "https://commons.wikimedia.org/wiki/Category:Woody_plants"
  ]
}
```

```json
{
    "id": "https://commons.wikimedia.org/wiki/Category:Woody_plants",
    "numberOfCategories": 5
}
```

An article is added to/removed from **Trees**:
```json
{
    "id": "https://commons.wikimedia.org/wiki/Category:Trees",
    "numberOfPages": 14
}
```
