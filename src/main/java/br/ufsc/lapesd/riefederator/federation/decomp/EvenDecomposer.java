package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class EvenDecomposer extends SourcesListAbstractDecomposer {
    @Inject
    public EvenDecomposer(@Nonnull Planner planner, @Nonnull PerformanceListener performance) {
        super(planner, performance);
    }

    @Override
    protected @Nonnull List<ProtoQueryNode> decomposeIntoProtoQNs(@Nonnull CQuery query) {
        try (TimeSampler ignored = Metrics.SELECTION_MS.createThreadSampler(performance)) {
            Metrics.AGGLUTINATION_MS.createThreadSampler(performance).close();
            return sources.stream()
                    .flatMap(s -> streamQueryNodes(s, s.getDescription().match(query)))
                    .collect(toList());
        }
    }

    private @Nonnull Stream<ProtoQueryNode> streamQueryNodes(@Nonnull Source source,
                                                             @Nonnull CQueryMatch m) {
        TPEndpoint ep = source.getEndpoint();
        return Stream.concat(
                m.getKnownExclusiveGroups().stream().map(g -> createPQN(ep, g)),
                m.getNonExclusiveRelevant().stream().map(t -> createPQN(ep, t)));
    }

    @Override
    public @Nonnull String toString() {
        return "EvenDecomposer";
    }
}
