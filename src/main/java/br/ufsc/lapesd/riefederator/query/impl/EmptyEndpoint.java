package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.*;

import javax.annotation.Nonnull;
import java.util.Collections;

import static java.util.stream.Collectors.toSet;

public class EmptyEndpoint extends AbstractTPEndpoint implements CQEndpoint {
    @Override
    public @Nonnull
    Results query(@Nonnull CQuery query) {
        return new CollectionResults(Collections.emptyList(),
                query.streamTerms(Var.class).map(Var::getName).collect(toSet()));
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        return Cardinality.UNSUPPORTED;
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        return true;
    }

    @Override
    public @Nonnull String toString() {
        return String.format("EmptyEndpoint@%x", System.identityHashCode(this));
    }
}
