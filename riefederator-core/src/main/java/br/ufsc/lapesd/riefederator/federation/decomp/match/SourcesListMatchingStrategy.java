package br.ufsc.lapesd.riefederator.federation.decomp.match;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticDescription;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.annotations.GlobalContextAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.indexed.ref.ImmRefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SourcesListMatchingStrategy implements MatchingStrategy {
    protected final @Nonnull List<Source> sources = new ArrayList<>();
    private final @Nonnull RefIndexSet<TPEndpoint> endpoints = new RefIndexSet<>();
    private final @Nonnull ImmRefIndexSet<TPEndpoint> immEndpoints = endpoints.asImmutable();
    private final @Nonnull List<Source> unmodifiableSources = Collections.unmodifiableList(sources);
    protected final @Nonnull PerformanceListener perfListener;

    @Inject
    public SourcesListMatchingStrategy(@Nonnull PerformanceListener perfListener) {
        this.perfListener = perfListener;
    }

    public SourcesListMatchingStrategy() {
        this(NoOpPerformanceListener.INSTANCE);
    }

    @Override public void addSource(@Nonnull Source source) {
        sources.add(source);
        endpoints.add(source.getEndpoint());
    }

    protected boolean match(@Nonnull Source source, @Nonnull CQuery query,
                            @Nonnull Agglutinator.State state) {
        Description description = source.getDescription();
        CQueryMatch match = description instanceof SemanticDescription
                ? ((SemanticDescription) description).semanticMatch(query)
                : description.match(query);
        state.addMatch(source.getEndpoint(), match);
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
            for (Source source : sources) {
                if (match(source, query, state))
                    nMatches++;
            }
            perfListener.sample(Metrics.SOURCES_COUNT, nMatches);
            GlobalContextAnnotation gCtx = query.getQueryAnnotation(GlobalContextAnnotation.class);
            return stampGlobalContext(state.takeLeaves(), gCtx);
        }
    }

    @Override public @Nonnull Collection<Source> getSources() {
        return unmodifiableSources;
    }

    @Override public @Nonnull ImmRefIndexSet<TPEndpoint> getEndpointsSet() {
        return immEndpoints;
    }
}
