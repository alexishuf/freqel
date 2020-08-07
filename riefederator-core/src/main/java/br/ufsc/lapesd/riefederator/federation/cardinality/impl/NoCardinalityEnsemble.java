package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

@Immutable
public class NoCardinalityEnsemble implements CardinalityEnsemble {
    public static final @Nonnull NoCardinalityEnsemble INSTANCE = new NoCardinalityEnsemble();

    public static class SingletonProvider implements Provider<CardinalityEnsemble> {
        @Override
        public NoCardinalityEnsemble get() {
            return INSTANCE;
        }
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint) {
        return Cardinality.UNSUPPORTED;
    }
}
