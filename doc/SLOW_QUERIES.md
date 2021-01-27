Slow executions
---------------

### B5 (797.801 ms):
```
SELECT  ?methylationCNTNAP2   WHERE {
 ?s affymetrix:x-symbol <http://bio2rdf.org/symbol:CNTNAP2>.
 ?s affymetrix:x-geneid ?geneId.
 ?geneId rdf:type tcga:expression_gene_lookup.
 ?geneId tcga:chromosome ?lookupChromosome.
 ?geneId tcga:start ?start.
 ?geneId tcga:stop  ?stop.
 ?uri tcga:bcr_patient_barcode ?patient .
 ?patient tcga:result ?recordNo .
 ?recordNo tcga:chromosome   ?chromosome.
 ?recordNo tcga:position     ?position.
 ?recordNo tcga:beta_value  ?methylationCNTNAP2.
 FILTER (?position >= ?start && ?position <= ?stop && str(?chromosome) = str(?lookupChromosome) )
}
```

Hypotheses:
- Cartesian product
- Slow FILTER evaluation (unlikely)

### B6 (562.101 ms):
```
SELECT  DISTINCT ?patient ?start ?stop ?geneExpVal
WHERE
{
	?s affymetrix:x-symbol <http://bio2rdf.org/symbol:KRAS>.
	?s affymetrix:x-geneid ?geneId.
	?geneId rdf:type tcga:expression_gene_lookup.
	?geneId tcga:chromosome ?lookupChromosome.
	?uri tcga:bcr_patient_barcode ?patient .
	?patient tcga:result ?recordNo .
	?recordNo tcga:chromosome   ?chromosome.
	?recordNo tcga:start ?start.
	?recordNo tcga:stop ?stop.
	?recordNo tcga:scaled_estimate ?geneExpVal
	FILTER (str(?lookupChromosome)= str(?chromosome))
}
```

Hypotheses:
- Cartesian product

### B7 (10922.151 ms)
```
SELECT DISTINCT ?patient ?p ?o WHERE {
  ?uri tcga:bcr_patient_barcode ?patient .
  ?patient dbpedia:country ?country.
  ?country dbpedia:populationDensity ?popDensity.
  ?patient tcga:bcr_aliquot_barcode ?aliquot.
  ?aliquot ?p ?o.
  FILTER(?popDensity >= 32)
}
```

Hypotheses:
- Bad filter placement: DISCARDED
- Filter not pushed to source: **FIXED**
- Bad plan: DISCARDED
- Bad join implementation choice: DISCARDED
- Lack of parallelism: **FIXED**
- Bind join executes same query more than once: 
  DISCARDED (not possible with left-deep plans) 

### S3
```
SELECT ?president ?party ?page WHERE {
   ?president <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/President> .
   ?president <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/United_States> .
   ?president <http://dbpedia.org/ontology/party> ?party .
   ?x <http://data.nytimes.com/elements/topicPage> ?page .
   ?x <http://www.w3.org/2002/07/owl#sameAs> ?president .
}
```

Hypothesis/bugs:
- PropertySelectivityCardinalityHeuristic over estimating conjunctions
  **FIXED**: fallbackSubjectPenalty was too large (100k -> 5k). No effect for S3
- fetchClasses=false: **FIXED**
  To make test execution tractable, building the indices is only done once. 
  Subsquent runs recover the predicate and classes sets from disk
- **FIXED**: cardinality guesses for unbound subject or unbound object with owl:sameAs 
  where overestimated. Actual cardinalities are small
- **bind join as implemented is inneficient -- VALUES?**

### bind join -> ASK degenaration

Affected queries:
- B5
- B6
- S3
- S4
- S10
- S11
- S12
- S13

For all affected queries, the degenerate side consists in ?x a ex:Class queries.
Therefore, the bind join cannot be safely replaced with a hash join. The 
implemented heuristic only replaces bind with hash when the degenerate side 
has at most double the cardinality of the non-degenerate side.

Plan comparison with FedX
-------------------------

Equivalent plans: S2, S3, S4, S6, S7, S11, S12 (ran 4.77% faster)


### Different plans

### S5
freqel's plan wrongly penalized this subquery: 
```
?film dbo:director ?director . 
?director dbo:nationality dbr:Italy .
```

With the introduction of `GeneralSelectivityHeuristic` and 
`BindJoinCardinalityEstimator`, freqel outputs the same plan as FedX.

#### S10
freqel's plan is 4.98% faster (3355.417ms vs. 3531.400ms). 
FedX plan is the reverse of the freqel plan:
```
                                       ⋈
                     +-----------------+-----------------+
                     ⋈                      ?Int db:interactionDrug1 ?y
         +-----------+----------+           ?Int db:interactionDrug2 ?Drug
 ?Drug a dbo:Drug     ?y ow:ameAs ?Drug     ?Int db:text ?           ?IntEffect
```

### S13
FedX plan
```
                                                                            ⋈
                                                         +------------------+-------------------+
                                                         ⋈                                      |
                                      +------------------+--------------+                       +
                                      ⋈                                 +        ?keggDrug dc:title ?title
                  +-------------------+-------------+       ?keggDrug kegg:xRef ?id
?drug db:drugcategory db:micronutrient    ?keggDrug a kegg:Drug
?drug db:casRegistryNumber ?id
```

FREQEL plan:
```
                                                                            ⋈
                                                         +------------------+-------------------+
                                                         ⋈                                      |
                                      +------------------+--------------+                       +
                                      ⋈                                 +        ?keggDrug dc:title ?title
                  +-------------------+-------------+       ?keggDrug a kegg:Drug
?drug db:drugcategory db:micronutrient    ?keggDrug kegg:xRef ?id
?drug db:casRegistryNumber ?id
```


