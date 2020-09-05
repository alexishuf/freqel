package br.ufsc.lapesd.riefederator.query.endpoint;

import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;

import javax.annotation.Nonnull;

public interface DisjunctiveProfile {
    boolean allowsUnion();
    boolean allowsJoin();
    boolean allowsProduct();
    boolean allowsUnfilteredProduct();
    boolean allowsUnfilteredProductRoot();
    boolean allowsFilter(@Nonnull SPARQLFilter filter);
    boolean allowsOptional();
    boolean allowsUnassignedQuery();
}
