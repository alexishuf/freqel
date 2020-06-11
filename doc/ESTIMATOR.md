Previous issue: when evaluating a join the cardinality estimated was the average of both
operands. This caused overestimations, since the effect of binding variables was ignored

Solution: to estimate a join, build a query that is a join of both nodes. Then estimate
cardinality of that query as if executed on a optimized SPARQL endpoint.

### Estimation steps

1. Create an estimation for every connected component.
   Connections can also occur over constants (for joins only variables serve as link)
   1.1. Create an estimation for every subject-object connected sub-component
        1.1.1. In parallel (but without concurrency) estimate triple cardinalities
               simulating bind joins from the subject and object boundaries towards the center.
               The center will be a term that divides the whole component in a subject-side and
               an object-side. Typically, one of the sides will have a smaller sum of
               cardinalities, meaning that it is the best side to start a join from.
        1.1.2. Re-estimate all triples from the loosing side starting using the
               state of the winning side. Exploration starts by queueing all 
               triples connected to any triple visited by by the winning side.
               (on some cases, the core term may be disconnected to some 
               triples of the loosing side, which connect to other triples of 
               the winning side) 
        1.1.3. Apply star selectivity bonus
        1.1.4. Apply double-ended selectivity bonus
        1.1.5. Aggregate all cardinalities with `sum()`
   1.2. Aggregate all estiamtes with `avg()`
        This is a middle ground between `sum()` and `min()`
2. Aggregate all component estimates with `(l, r) -> max(max(l, r)*4, fallbackSubjectPenalty)`
   Theoretically, one should use `l*r`, but that overflows too quickly

#### Star selectivity bonus

`?x ex:p ?o` triples are dangerous and therefore penalized by the estimator. 
However, this penalty can be reduced for stars such as `?x ex:p1 ex:o; ex:p2 ?o`,
since the `?x ex:p1 ex:o` triple greatly reduces the answer set for 
`?x ex:p2 ?o`

Steps:

1. Find every star
2. Compute the rate (in `[0,1]`) of triples with unbound objects
3. Multiply the cardinality of every triple with unbound objects by that factor 

#### Double-ended selectivity bonus

Paths with both the subject and objects bounded are more efficient. Cardinality 
estimation for inner triples consider the bind effect of only one direction. 
This bonus aims to correct that by applying a factor in `[0,1]` to the inner 
triples' cardinalities.  

Consider for example the following path:
```
:Alice :knows ?x  ---  ?x :knows ?y  +--  ?y :name "Bob"
                                     +--  ?y :age  ?age 
```

The bounded rate is 1.0 at the subject side and 0.5 at the object side. 
Thus, the factor 1 / (1+0.5) = 0.66 is applied to the inner node `?x :knows ?y`.


