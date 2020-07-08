JSON-LD context for R2R mapping
===============================

This particular relational to RDF mapping strategy uses JSON-LD context files,  
and is an extension to the one used in the [csv2rdf](https://github.com
/ivansalvadori/csv2rdf) Web API service. A JSON-LD `@context` definition
works as a map from column names to URIs. The URIs of records made into RDF
subjects can be controlled with a special  key (or pseudo-column) 
`@uriProperty` which can reference another column or use `"@GenerateUri"` to 
simply mint sequentially-numbered URIs.

Example:
```json
{
  "@context": {
    "@type": "http://example.org/ns#Person",
    "@uriProperty": "id",
    "id": "http://example.org/ns#id",
    "name": "http://example.org/ns#name"
  },
  "@instancePrefix": "http://any.example.org/uri/prefix/"
}
```

Notes:
- `@instancePrefix` is equivalent to `@resourceDomain` (used in csv2rdf)
- Columns mentioned  in `@uriProperty` will still appear in the RDF 
  representation

Extensions to JSON-LD Contexts (also used in csv2rdf)
-----------------------------------------------------

The only extension within `@context` is `@uriProperty`. This defines how to 
mint the URI of subjects created for every record. Possible values:
- `@GenerateUri`: Will mint URIs adding a sequence number to `@instancePrefix`
- `@blank`: Will use blank nodes instead of minting URIs.
- `column`: Will concatenate the value of `column` to `@instancePrefix`. 
  If the values are known to already be URIs, then do not provide a 
  `@instancePrefix`. This will use the URIs in `column` to be used.
- `["colA", "colB"]`: This will mint URIS with the following template: 
  `{@instancePrefix}{colA}-{colB}`. The separator (`-`) is controlled with 
  `@uriPropertySeparator` (see below)

The default value of `@uriProperty` is `@blank`.  

Other properties appear as siblings of `@context`:
- `@fallbackPrefix`: For any column not mapped in `@context`, it will be mapped
  to a URI generated as `{@fallbackPrefix}/{columnName}`. The **default value** 
  is `urn:plain:`. To disable this implicit mapping, set `@fallbackPrefix` to null.
- `@instancePrefix` (or `@resourceDomain`): The prefix of each RDF subject 
  generated for each relational record The full URI is assembled by appending 
  either a sequence number or some values from some columns to this prefix.
  The **default value** is `null`. If  `@uriProperty` is defined it should 
  yield a valid URI. If `@uriProperty` is not present then each record will 
  be represented as a blank node.   
- `@uriPropertySeparator` When `@uriProperty`is an array with more than one 
  column name, this defines how to separate values from multiple columns in 
  the generated URI. The **default value** is `-`.
- `@tableName` determines the source table name being described. 
   This is optional and defaults to `null`.
- `@nonExclusive` makes the generated Molecules, used in matching processes 
   to not consider the RDF-subjects to be complete descriptions. By default 
   the subjects are considered complete. This allows for larger subqueries 
   to match leading to more efficient execution. However this may eliminate 
   the possibility of some inter-source joins if this assumption is incorrect. 
   Set `nonExclusive` to true in order to fix this.s  
  
### Extensions with respect to csv2rdf

- `@` is optional for all properties except `@context`and `@uriProperty`. 
  csv2rdf requires the `@`for all
- `@instancePrefix` added as alias to `@resourceDomain`
- Guesses for `@uriProperty` and `@instancePrefix`  
- `@fallbackPrefix`: not present in csv2rdf
- `@uriPropertySeparator`: not present in csv2rdf
- `@tableName`: not present in csv2rdf
- `@nonExclusive`: not present in csv2rdf
