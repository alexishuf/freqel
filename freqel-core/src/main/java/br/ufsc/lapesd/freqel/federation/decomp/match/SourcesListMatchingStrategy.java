package br.ufsc.lapesd.freqel.federation.decomp.match;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.MatchReasoning;
import br.ufsc.lapesd.freqel.description.semantic.AlternativesSemanticDescription;
import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.GlobalContextAnnotation;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.indexed.ref.ImmRefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collection;

public class SourcesListMatchingStrategy implements MatchingStrategy {
    protected final @Nonnull RefIndexSet<TPEndpoint> endpoints = new RefIndexSet<>();
    private final @Nonnull ImmRefIndexSet<TPEndpoint> immEndpoints = endpoints.asImmutable();
    protected final @Nonnull PerformanceListener perfListener;

    @Inject
    public SourcesListMatchingStrategy(@Nonnull PerformanceListener perfListener) {
        this.perfListener = perfListener;
    }

    public SourcesListMatchingStrategy() {
        this(NoOpPerformanceListener.INSTANCE);
    }

    @Override public void addSource(@Nonnull TPEndpoint endpoint) {
        endpoints.add(endpoint);
    }

    protected boolean match(@Nonnull TPEndpoint source, @Nonnull CQuery query,
                            @Nonnull Agglutinator.State state) {
        Description description = source.getDescription();
        CQueryMatch match = description instanceof AlternativesSemanticDescription
                ? ((AlternativesSemanticDescription) description).semanticMatch(query)
                : description.match(query, MatchReasoning.NONE);
        state.addMatch(source, match);
        return !match.isEmpty();
    }

    protected @Nonnull Collection<Op> stampGlobalContext(@Nonnull Collection<Op> collection,
                                                         @Nullable GlobalContextAnnotation gCtx) {
        if (gCtx == null)
            return collection;
        for (Op op : collection) {
            if (op instanceof EndpointQueryOp) {
                MutableCQuery q = ((EndpointQueryOp) op).getQuery();
                assert !q.hasQueryAnnotations(GlobalContextAnnotation.class);
                q.annotate(gCtx);
            } else if (op instanceof UnionOp) {
                stampGlobalContext(op.getChildren(), gCtx);
            }
        }
        return collection;
    }

    @Override public @Nonnull Collection<Op> match(@Nonnull CQuery query,
                                                   @Nonnull Agglutinator agglutinator) {
        int nMatches = 0;
        try (TimeSampler ignored = Metrics.SELECTION_MS.createThreadSampler(perfListener)) {
            Agglutinator.State state = agglutinator.createState(query);
            for (TPEndpoint source : endpoints) {
                if (match(source, query, state))
                    nMatches++;
            }
            perfListener.sample(Metrics.SOURCES_COUNT, nMatches);
            GlobalContextAnnotation gCtx = query.getQueryAnnotation(GlobalContextAnnotation.class);
            return stampGlobalContext(state.takeLeaves(), gCtx);
        }
    }

    @Override public @Nonnull ImmRefIndexSet<TPEndpoint> getEndpoints() {
        return immEndpoints;
    }
}
