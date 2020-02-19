riefederator 
============

This is a federated query mediator highly inspired by mediator systems that 
target RDF sources (SPARQL and TPF endpoints). In addition to federating 
queries in such scenario, it also aims to provide the following features:

- [x] Web APIs as queryable sources (JSON or RDF)
- [ ] SPARQL 1.1 path queries 
- [ ] Query time reasoning

Project status is under high-frequency development. **Expect things to
break horribly in surprisingly painful ways**.

Building & dependency information
--------

Use the maven wrapper to build and test. Tests may take a while (at least 1min).

```shell script
./mvnw
```

Currently, the project is not mature enough for publication in maven central. 
Meanwhile, include it as a git submodule and depend on it as a sibling 
submodule of your application, OR install to a local repository. In both 
cases, dependency information is the following:

```xml
<dependency>
  <groupId>br.ufsc.lapesd.riefederator</groupId>
  <artifactId>riefederator</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

How to use it
-------------

Currently, the user interface is the `Federation` class. To get up and 
running, try this:

```java
Collection<Source> sources; // sources to query
List<Solution> listOfSolutions; //where to save results

// Setup the federation
Federation federation = Federation.createDefault();
sources.forEach(federation::addSource);
federation.setEstimatePolicy(EstimatePolicy.local(100));

// query the graph that is a union of all sources
Results results = federation.query(aCQuery); //plan
results.forEachRemainingThenClose(listOfSolutions::add); //consume all results
```