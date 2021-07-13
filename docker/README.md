freqel docker image
===================

This image bundles the `freqel-server` executable jar into an 
`openjdk:11.0.11-buster` image together with binaries for [VLog][vlog] and 
[hdt-cpp][hdtcpp].

Default entrypoint is /usr/loca/bin/freqel-server and default `CMD` is 
`--config /config/federation.yaml`. Note that there is no default 
`/config/federation.yaml` file. Use the example on the main freqel 
[README](../README.md) to create one such file and mount the `/config` volume. 

[vlog]: https://github.com/karmaresearch/vlog/
[hdtcpp]: https://github.com/rdfhdt/hdt-cpp

Full federation example
-----------------------

The following steps show how to use freqel over two SPARQL endpoints. The 
examples below use a docker [image][fuseki-img] of [Apache Jena Fuseki][fuseki],
but the `fuseki-server` launcher script from the binary 
[distribution][fuseki-download] can be used instead of docker run using the 
same arguments (after the image name).

Run one Fuseki instance at port 3031:
```bash
cd "$(mktemp -d)"
cat >data.ttl<<EOF
@prefix ex: <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
ex:Alice a foaf:Person;
  foaf:name "Alice"@en;
  foaf:knows ex:Bob.
EOF
docker run -it --rm -v $(pwd):/data -p 3031:3031 alexishuf/fuseki:3.17.0 \
    --port 3031 --file /data/data.ttl /a
```

... and another at port 3032:
```bash
cd "$(mktemp -d)"
cat >data.ttl<<EOF
@prefix ex: <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
ex:Bob foaf:name "Bob"@en;
  foaf:age 23.
ex:Alice foaf:age 25.
EOF
docker run -it --rm -v $(pwd):/data -p 3032:3032 alexishuf/fuseki:3.17.0 \
    --port 3032 --file /data/data.ttl /b
```
Create a `federation.yaml` file:
```bash
cd "$(mktemp -d)"
cat >federation.yaml<<EOF
sources:
  - loader: sparql
    uri: http://172.17.0.1:3031/a/sparql
  - loader: sparql
    uri: http://172.17.0.1:3032/b/sparql
EOF
```

> 172.17.0.1 is the docker host address, get it with `ip address show docker0`
> or similar, depending on your setup.

Start the freqel endpoint (from the same dir with the `federation.yaml`):
```bash
docker run -it --rm -v $(pwd):/config -p 4040:4040 \
    alexishuf/freqel:latest \
    --address 0.0.0.0 --port 4040 --config /config/federation.yaml
```

> All arguments after the image name in the example above correspond to default 
> values in freqel-server or the docker image (in the case of `--config`).

Query the endpoint at `http://localhost:4040/sparql/query`:
```bash
curl -X POST -H "Accept: text/tab-separated-values" \
    -H "Content-Type: application/sparql-query" \
    --data @- http://localhost:4040/sparql/query <<EOF
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT * WHERE {
  ?s foaf:name ?o ;
     foaf:age 25.
}
EOF
```

... which should output:
```text
?s      ?o
<http://example.org/Alice>      "Alice"@en
```
