package br.ufsc.lapesd.riefederator.federation.decomp.match;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.indexed.ref.ImmRefIndexSet;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface MatchingStrategy {
    void addSource(@Nonnull Source source);
    @Nonnull Collection<Op> match(@Nonnull CQuery query, @Nonnull Agglutinator agglutinator);
    @Nonnull Collection<Source> getSources();
    @Nonnull ImmRefIndexSet<TPEndpoint> getEndpointsSet();
}
