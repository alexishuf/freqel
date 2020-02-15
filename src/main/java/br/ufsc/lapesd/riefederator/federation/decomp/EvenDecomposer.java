package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class EvenDecomposer extends ListSourcesAbstractDecomposer {
    @Inject
    public EvenDecomposer(@Nonnull Planner planner) {
        super(planner);
    }

    @Override
    public @Nonnull List<QueryNode> decomposeIntoLeaves(@Nonnull CQuery query) {
        return sources.stream()
                .flatMap(s -> streamQueryNodes(s, s.getDescription().match(query)))
                .collect(toList());
    }

    @Override
    public @Nonnull String toString() {
        return "EvenDecomposer";
    }

    private @Nonnull Stream<QueryNode> streamQueryNodes(@Nonnull Source source,
                                                        @Nonnull CQueryMatch m) {
        TPEndpoint ep = source.getEndpoint();
        return Stream.concat(
                m.getKnownExclusiveGroups().stream().map(g -> createQN(ep, g)),
                m.getNonExclusiveRelevant().stream().map(t -> createQN(ep, t)));
    }
}
