# DGraph

## Local setup

### Standalone (easier)

```shell
docker pull dgraph/standalone
DATA_HOME=/srv/data/dgraph
docker run --rm -it -p "8080:8080" -p "9080:9080" -p "8000:8000" -v $DATA_HOME:/dgraph "dgraph/standalone:v21.03.2"
```

### Multi-node setup
```shell
# get the docker image
docker pull dgraph/dgraph:latest
# obtain your IP address
IPADRESS=192.168.1.113
# create a docker network
docker network create dgraph_default
# create a folder for dgraph
ZERO_HOME=/srv/dgraph/zero
mkdir -p $ZERO_HOME
# start dgraph zero
docker run -it -p 5080:5080 --network dgraph_default -p 6080:6080 -v $ZERO_HOME:/dgraph dgraph/dgraph:v21.03.2 dgraph zero --my=$IPADRESS:5080

SERVER1=/srv/dgraph/srv1
mkdir -p $SERVER1
# start dgraph alpha server1
docker run -it -p 7080:7080 --network dgraph_default -p 8080:8080 -p 9080:9080 -v $SERVER1:/dgraph dgraph/dgraph:v21.03.2 dgraph alpha --zero=$IPADRESS:5080 --my=$IPADRESS:7080

SERVER2=/srv/dgraph/srv2
mkdir -p $SERVER2
# start dgraph alpha server1
docker run -it -p 7081:7081 --network dgraph_default -p 8081:8081 -p 9081:9081 -v $SERVER2:/dgraph dgraph/dgraph:v21.03.2 dgraph alpha --zero=$IPADRESS:5080 --my=$IPADRESS:7081  -o=1
```

## Schema creation

```shell
# reset all data: curl -X POST localhost:8080/alter -d '{"drop_all": true}'

# GraphQL schema
curl -X POST localhost:8080/admin/schema --data-binary @categ_schema.gql
# index creation (still unclear why it's not possible to do in one step)
curl -XPOST  'localhost:8080/alter' --data-binary @categ_schema_props.schema
```

Get the schema
```shell
curl -H "Content-Type: application/json" --data-binary '{"query":"{\n getGQLSchema {\n schema\n generatedSchema\n }\n}","variables":{}}' localhost:8080/admin | jq .
```
## Import

With json dgraph does not create the links because it needs to fetch the uid
```shell
curl -H "Content-Type: application/json" -XPOST --data-binary @datapoint.json localhost:8080/mutate | jq .
```

Using RDF is possible with blank nodes for the UID but reference-able in a single doc (requires all data in a single POST request):
```shell
curl -H "Content-Type: application/rdf" -XPOST --data-binary @datapoint.rdf localhost:8080/mutate | jq .
```

Using the import script and the python client (doing 2 passes):
```shell
python import.py https://people.wikimedia.org/~dcausse/mediawikiwiki-20211002-categories.json.gz
```

## Queries

Using graphql and the queries generated from the schema:
```shell
curl -H "Content-Type: application/graphql" localhost:8080/graphql -XPOST -d @point_query.gql | jq .
```

Using dql:
```shell
curl -H "Content-Type: application/dql" localhost:8080/query -XPOST -d @point_query.dql | jq .
```
