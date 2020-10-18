package br.ufsc.lapesd.riefederator.federation.decomp.deprecated;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticDescription;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.decomp.ProtoQueryOp;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class EvenDecomposer extends SourcesListAbstractDecomposer {
    private static final Logger logger = LoggerFactory.getLogger(EvenDecomposer.class);

    @Inject
    public EvenDecomposer(@Nonnull ConjunctivePlanner planner, @Nonnull PerformanceListener performance) {
        super(planner, performance);
    }

    @Override
    protected @Nonnull List<ProtoQueryOp> decomposeIntoProtoQNs(@Nonnull CQuery query) {
        if (sources.stream().anyMatch(s -> s.getDescription() instanceof SemanticDescription)) {
            logger.warn("EvenDecomposer does not support semantic matches (yet!). " +
                        "Rewritings will be ignored");
        }
        try (TimeSampler ignored = Metrics.SELECTION_MS.createThreadSampler(performance)) {
            Metrics.AGGLUTINATION_MS.createThreadSampler(performance).close();
            return sources.stream()
                    .flatMap(s -> streamQueryNodes(query, s, s.getDescription().match(query)))
                    .collect(toList());
        }
    }

    private @Nonnull Stream<ProtoQueryOp>
    streamQueryNodes(@Nonnull CQuery query, @Nonnull Source source, @Nonnull CQueryMatch m) {
        TPEndpoint ep = source.getEndpoint();
        IndexSet<Triple> triplesUniverse = query.attr().triplesUniverseOffer();
        IndexSet<String> varsUniverse = query.attr().varNamesUniverseOffer();
        return Stream.concat(
                m.getKnownExclusiveGroups().stream().map(g -> {
                    if (triplesUniverse != null)
                        g.attr().offerTriplesUniverse(triplesUniverse);
                    if (varsUniverse != null)
                        g.attr().offerVarNamesUniverse(varsUniverse);
                    return new ProtoQueryOp(ep, g);
                }),
                m.getNonExclusiveRelevant().stream().map(t -> {
                    CQuery q = CQuery.from(t);
                    if (triplesUniverse != null)
                        q.attr().offerTriplesUniverse(triplesUniverse);
                    if (varsUniverse != null)
                        q.attr().offerVarNamesUniverse(varsUniverse);
                    return new ProtoQueryOp(ep, q);
                }));
    }

    @Override
    public @Nonnull String toString() {
        return "EvenDecomposer";
    }
}
