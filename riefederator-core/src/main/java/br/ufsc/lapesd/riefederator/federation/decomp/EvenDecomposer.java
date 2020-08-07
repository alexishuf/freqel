package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticDescription;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
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
    public EvenDecomposer(@Nonnull Planner planner, @Nonnull PerformanceListener performance) {
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
                    .flatMap(s -> streamQueryNodes(s, s.getDescription().match(query)))
                    .collect(toList());
        }
    }

    private @Nonnull Stream<ProtoQueryOp> streamQueryNodes(@Nonnull Source source,
                                                           @Nonnull CQueryMatch m) {
        TPEndpoint ep = source.getEndpoint();
        return Stream.concat(
                m.getKnownExclusiveGroups().stream().map(g -> new ProtoQueryOp(ep, g)),
                m.getNonExclusiveRelevant().stream()
                        .map(t -> new ProtoQueryOp(ep, CQuery.from(t))));
    }

    @Override
    public @Nonnull String toString() {
        return "EvenDecomposer";
    }
}
