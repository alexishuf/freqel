package br.ufsc.lapesd.freqel.query.endpoint;

import br.ufsc.lapesd.freqel.query.modifiers.SPARQLFilter;

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
