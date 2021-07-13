freqel 
============

This is a federated query mediator highly inspired by mediator systems that 
target RDF sources (SPARQL and TPF endpoints). In addition to federating 
queries in such scenario, it also aims to provide the following features:

- [x] Web APIs as queryable sources (JSON or RDF)
- [ ] SPARQL 1.1 path queries 
- [ ] Query time reasoning

Project status is under high-frequency development. **Expect things to
break horribly in surprisingly painful ways**.

Build and tests
---------------

Use the maven wrapper to build and test: `./mvnw`. Running all tests will take 
around 30min. For machines with limitted RAM size or slow hard drives 
(e.g., notebooks), tests might take more than a hour or may fail due to 
`OutOfMemoryException`s on the parent JVM or on child JVMs spawned during tests

To get jars quickly, skip tests: `./mvnw package -DskipTests=true`. For 
development purposes, fast-running tests are under the "fast" TestNG group and
take around 20 seconds for the slowest module (`freqel-tests`). From the 
command-line, run fast tests with `./mvnw verify -Dgroups=fast`.


How to use it
-------------

There are two main use cases:

1. Using the federation as a library
2. Running the federation as an SPARQL endpoint

### Using FREQEL as a library

First, include the freqel-core dependency:
```xml
<dependency>
  <groupId>br.ufsc.lapesd.freqel</groupId>
  <artifactId>freqel-core</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

For additional functionality, you might consider the following modules:
- `freqel-cassandra`: Add support for cassandra as a source
- `freqel-hdt`: Adds support for HDT as a source or as a TBox
- `freqel-webapis`: Adds support for swagger-described web apis as sources. You might need to use some [Swagger extensions](doc/SWAGGER_EXT.md) (which are non-intrusive).
- `freqel-vlog`: Use [VLog](https://github.com/karmaresearch/vlog) as a TBox materialization reaasoners 
- `freqel-owlapi`: Use HermIT and JFact as TBox materialization reasoners

With dependencies on the classpath, create a federation, add sources 
and send queries:
```java
Federation federation = Freqel.createFederation();
federation.addSource(new SPARQLClient("https://example.org/sparql/query"));
Op query = SPARQLParser.tolerant().parse("SELECT ....");
try (Results results = federation.query(query)) {
    while (results.hasNext())
    doStuffWith(results.next().get("varName"));
}
```

In addition to `SPARQLClient`, additional sources can be created with the following classes:
- `ARQEndpoint` (Apache jena sources, includes a SPARQL client)
- `HDTEndpoint` (queries HDT files)
- `CassandraCQEndpoint` (client for Apache cassandra)
- `JDBCCQEndpoint` (query SQL sources)
- `SwaggerParser` (creates `WebAPICQEndpoint` from swagger descriptions)

Alternatively, one may place all source specifications along with other 
federation engine configurations inside a yaml/json file:
```java
Federation federation = Freqel.createFederation(specFile);
// addSource() calls already done as configured in specFile!
```

A short example of such files in the section below (running as an
SPARQL endpoint). For details of such spec files, 
see [the reference](doc/CONFIG.md).

### Running the federation as an SPARQL endpoint

In this mode, configuration stays on a JSON or YAML file:

```shell
freqel-server/target/freqel-server --config federation.yaml
```

> The freqel-server binary is just a shell script concatenated with 
> `freqel-server-1.0-SNAPSHOT.jar` that `java ${JVM_ARGS} -jar ` itself.
> That is, you may pass options to the JVM setting the JVM_ARGS environment 
> variable or you may use that file as an argument to the `-jar` flag.

This will run a SPARQL endpoint listening at 
[http://127.0.0.1:4040/sparql/query](http://127.0.0.1:4040/sparql/query). 

A `federation.yaml` file looks like this:
```yaml
# Will store source descriptions inside a dir named cache
# All relative paths in this file are relative to the file's dir 
sources-cache-dir: cache
# Perform late reasoning via rewriting when requested
endpoint-reasoner: HeuristicEndpointReasoner
# Use an already materialized TBox in a queryable HDT file
# tbox-hdt: materialized-ontology.hdt
# ... or get a non-materialized ontologies
materializer-spec: 
  - onto1.ttl
  - onto2.rdf
# ... feed them to this TBoxMaterializer implementation (could be a FQCN)
materializer: SystemVLogMaterializer
# ... and store the results in this directory
materializer-storage: materialization
# Sources in the federation
sources: 
  # Load all triples in all files into a single graph. Any RDF syntax goes
  - loader: rdf-file
    file: anything.ttl
    files: 
      - file1.nt
      - file2.nq
    urls:
      - http://example.org/file1.ttl
      - http://example.org/file2.jsonld
  # Open HDT files for querying (instead of ignoring HDT indices)
  - loader: hdt
    file: file.hdt
  - loader: sparql
    # Eagerly build a index, instead of lazy ASK queries (the default)
    description: select
    uri: http://example.org/sparql/query
  - loader: swagger
    # Extensions file that links to the Web API authoritative swagger
    file: extensions.yaml
```

The configuration keys at the root, with the exception of `sources` are 
configurations for the `FreqelConfig` class. These configuration keys could 
also be set using java properties, environment variables and specially located
`freqel-config.{yaml,json,properties}` files. See [CONFIG.md](doc/CONFIG.md) 
for more details. 
