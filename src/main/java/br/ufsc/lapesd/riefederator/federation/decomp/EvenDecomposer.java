package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
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
    protected @Nonnull List<ProtoQueryNode> decomposeIntoProtoQNs(@Nonnull CQuery query) {
        return sources.stream()
                .flatMap(s -> streamQueryNodes(s, s.getDescription().match(query)))
                .collect(toList());
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
