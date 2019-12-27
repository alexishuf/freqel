package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.HeuristicPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class EvenDecomposer implements DecompositionStrategy {
    private final @Nonnull List<Source> sources = new ArrayList<>();
    private final @Nonnull Planner planner = new HeuristicPlanner();

    @Override
    public void addSource(@Nonnull Source source) {
        sources.add(source);
    }

    @Override
    public @Nonnull PlanNode decompose(@Nonnull CQuery query) {
        List<QueryNode> leafs = sources.stream()
                .flatMap(s -> streamQueryNodes(s, s.getDescription().match(query)))
                .collect(toList());
        return planner.plan(leafs);
    }

    private @Nonnull Stream<QueryNode> streamQueryNodes(@Nonnull Source source,
                                                        @Nonnull CQueryMatch m) {
        TPEndpoint ep = source.getEndpoint();
        return Stream.concat(
                m.getKnownExclusiveGroups().stream().map(g -> new QueryNode(ep, g)),
                m.getNonExclusiveRelevant().stream().map(t -> new QueryNode(ep, CQuery.from(t))));
    }
}
