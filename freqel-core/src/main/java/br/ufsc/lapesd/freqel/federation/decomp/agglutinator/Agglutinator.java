package br.ufsc.lapesd.freqel.federation.decomp.agglutinator;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface Agglutinator {
    interface State {
        void addMatch(@Nonnull TPEndpoint ep, @Nonnull CQueryMatch match);
        @Nonnull Collection<Op> takeLeaves();
    }
    @Nonnull State createState(@Nonnull CQuery query);
    void setMatchingStrategy(@Nonnull MatchingStrategy strategy);
}
