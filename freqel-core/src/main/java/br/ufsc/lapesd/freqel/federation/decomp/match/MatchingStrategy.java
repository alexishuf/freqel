package br.ufsc.lapesd.freqel.federation.decomp.match;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.description.Source;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.indexed.ref.ImmRefIndexSet;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface MatchingStrategy {
    void addSource(@Nonnull Source source);
    @Nonnull Collection<Op> match(@Nonnull CQuery query, @Nonnull Agglutinator agglutinator);
    @Nonnull Collection<Source> getSources();
    @Nonnull ImmRefIndexSet<TPEndpoint> getEndpointsSet();
}
