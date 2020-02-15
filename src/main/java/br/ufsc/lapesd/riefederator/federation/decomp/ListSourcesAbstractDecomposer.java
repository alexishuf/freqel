package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class ListSourcesAbstractDecomposer implements DecompositionStrategy {
    protected final @Nonnull List<Source> sources = new ArrayList<>();
    protected final @Nonnull Planner planner;
    protected int estimatePolicy = 0;

    public ListSourcesAbstractDecomposer(@Nonnull Planner planner) {
        this.planner = planner;
    }

    @Override
    public void addSource(@Nonnull Source source) {
        sources.add(source);
    }

    @Override
    public void setEstimatePolicy(int estimatePolicy) {
        this.estimatePolicy = estimatePolicy;
    }

    @Override
    public @Nonnull ImmutableCollection<Source> getSources() {
        return ImmutableList.copyOf(sources);
    }

    @Override
    public @Nonnull PlanNode decompose(@Nonnull CQuery query) {
        Collection<QueryNode> leafs = decomposeIntoLeaves(query);
        return planner.plan(query, leafs);
    }

    protected @Nonnull QueryNode createQN(@Nonnull TPEndpoint ep, @Nonnull CQuery query) {
        return new QueryNode(ep, query, ep.estimate(query, estimatePolicy));
    }

    protected @Nonnull QueryNode createQN(@Nonnull TPEndpoint ep, @Nonnull Triple triple) {
        return createQN(ep, CQuery.from(triple));
    }

}
