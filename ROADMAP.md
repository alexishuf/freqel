Roadmap
=======

Weeks:
1. **09/12**. Molecule & even. Start reasoning over molecule
2. **16/12**. End Reasoning over Molecule and over even and Aggregation 
3. **23/12**. Planning & execution. Integrate CNPJ services.
4. **30/12**. SIAFI/SIASG & wikidata 
5. **06/01**. Writing / experiments
6. **13/01**. Writing

Federation
----------

1. Source selection
    1. Molecule 
    2. Even
    3. Reasoning over Molecule.
       Only consider subPropertyOf / subClassOf as derived beforehand by VLog/other.
    4. Reasoning over Even. As simple as for Molecule
2. Aggregation for then non-exclusive triples
3. Planning
    1. Heuristics
    2. ~~Join Algorithm selection~~ (bind vs. **hash**). Nearly no federated 
       mediator uses hash-join or other table-based joins  
4. Execution (graph of processors)
5. Application
    1. Portal da transparência -- CNPJ
    2. Compras do governo (SIAFI/SIASG via SUS)  
    3. Link product-like terms with wikidata 

Reasoning
---------

1. Rule processors for class/property taxonomies
2. Rule parsing
3. Processors for custom rules
4. Processors for remaining OWL rules

