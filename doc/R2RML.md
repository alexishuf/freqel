R2RML Support
=============

R2RML support exists as an implementation of RelationalMapping. 
To load a R2RML mapping, use the builder in RRMapping:

```
RRMapping mapping = RRMapping.builder().strict(true)
                             .baseURI("http://example.com/base/")
                             .load(pathToRDFFile);
```

There are alternatives to `load` using an `InputStream` (provide the RDF 
language using `.lang()`) and there is a `loadFromURI()` method that fetches
the mapping from an URI. To obtain a usable endpoint, use the `RRMapping` 
with something like `JDBCCQEndpoint.builder(theMapping).connectingTo(jdbcUrl)`.

Nearly all of R2RML is supported, with two notable exceptions:

- Primary key enforcement during mapping: Duplicates are not removed. W3C 
  R2RML test cases use rr:template together with rr:termType rr:BlankNode
  to avoid creating one blank node for each SQL result row when those rows 
  are themselves duplicates. This could be enforced, but at the cost of 
  making the RelationalMapping interface stateful.
- Quads: Since the mediator only works with triples, quad output is not 
  possible. R2RML mappings that define output graphs will have the context 
  (graph) mapping **silently** ignored. 
