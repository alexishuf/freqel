package br.ufsc.lapesd.riefederator.federation.decomp.agglutinator;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EvenAgglutinator implements Agglutinator {
    private final @Nonnull PerformanceListener perfListener;

    @Inject
    public EvenAgglutinator(@Nonnull PerformanceListener perfListener) {
        this.perfListener = perfListener;
    }

    public EvenAgglutinator() {
        this(NoOpPerformanceListener.INSTANCE);
    }

    @Override public @Nonnull State createState(@Nonnull CQuery query) {
        return new EvenState(query);
    }

    @Override public void setMatchingStrategy(@Nonnull MatchingStrategy strategy) {
        /* pass */
    }

    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }

    protected class EvenState extends AbstractAgglutinatorState {
        private List<Op> list = new ArrayList<>();
        private List<Op> syncList = Collections.synchronizedList(list);

        protected EvenState(@Nonnull CQuery query) {
            super(query);
        }

        private @Nonnull Op toOp(@Nonnull TPEndpoint ep,
                                 @Nonnull Collection<CQuery> queries) {
            int size = queries.size();
            if (size > 1) {
                ArrayList<Op> list = new ArrayList<>(size);
                for (CQuery query : queries)
                    list.add(new EndpointQueryOp(ep, addUniverse(query)));
                return  UnionOp.build(list);
            } else {
                assert size == 1;
                return new EndpointQueryOp(ep, addUniverse(queries.iterator().next()));
            }
        }

        private @Nonnull Op toOp(@Nonnull TPEndpoint ep, @Nonnull Triple fallback,
                                 @Nonnull Collection<CQuery> queries) {
            if (queries.isEmpty())
                return new EndpointQueryOp(ep, addUniverse(CQuery.from(fallback)));
            else
                return toOp(ep, queries);
        }

        private @Nonnull Op toOp(@Nonnull TPEndpoint ep, CQuery fallback,
                                 @Nonnull Collection<CQuery> queries) {
            if (queries.isEmpty())
                return new EndpointQueryOp(ep, addUniverse(fallback));
            else
                return toOp(ep, queries);
        }

        @Override public void addMatch(@Nonnull TPEndpoint ep, @Nonnull CQueryMatch match) {
            if (match instanceof SemanticCQueryMatch) {
                SemanticCQueryMatch sm = (SemanticCQueryMatch) match;
                for (CQuery eg : match.getKnownExclusiveGroups())
                    syncList.add(toOp(ep, eg, sm.getAlternatives(eg)));
                for (Triple triple : sm.getNonExclusiveRelevant())
                    syncList.add(toOp(ep, triple, sm.getAlternatives(triple)));
            } else {
                for (CQuery eg : match.getKnownExclusiveGroups())
                    syncList.add(new EndpointQueryOp(ep, addUniverse(eg)));
                for (Triple triple : match.getNonExclusiveRelevant())
                    syncList.add(new EndpointQueryOp(ep, addUniverse(CQuery.from(triple))));
            }
        }

        @Override public @Nonnull Collection<Op> takeLeaves() {
            Metrics.AGGLUTINATION_MS.createThreadSampler(perfListener).close();
            return list;
        }
    }
}
