package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.EstimatePolicy;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class ListSourcesAbstractDecomposer implements DecompositionStrategy {
    protected final @Nonnull List<Source> sources = new ArrayList<>();
    protected final @Nonnull Planner planner;
    protected int estimatePolicy = EstimatePolicy.local(50);

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
        FilterAssigner p = new FilterAssigner(query, this::createQN);
        Collection<QueryNode> leafs = decomposeIntoLeaves(query, p);
        PlanNode plan = planner.plan(query, leafs);
        p.placeBottommost(plan);
        return plan;
    }

    @Override
    public @Nonnull List<QueryNode> decomposeIntoLeaves(@Nonnull CQuery query) {
        return decomposeIntoLeaves(query, new FilterAssigner(query, this::createQN));
    }

    public @Nonnull List<QueryNode> decomposeIntoLeaves(@Nonnull CQuery query,
                                                              @Nonnull FilterAssigner placement) {
        List<ProtoQueryNode> list = decomposeIntoProtoQNs(query);
        return placement.placeFiltersOnLeaves(list);
    }

    /**
     * Decompose the query into a list of {@link ProtoQueryNode}.
     * The result of this method will undergo filter distribution at a later stage.
     *
     * @param query The query to decompose.
     * @return a List of non-null {@link ProtoQueryNode} instances. All triples in query must
     *         appear in at least one of the {@link ProtoQueryNode}s.
     */
    protected abstract @Nonnull List<ProtoQueryNode> decomposeIntoProtoQNs(@Nonnull CQuery query);

    protected @Nonnull ProtoQueryNode createPQN(@Nonnull TPEndpoint endpoint,
                                                @Nonnull CQuery query) {
        return new ProtoQueryNode(endpoint, query);
    }

    protected @Nonnull ProtoQueryNode createPQN(@Nonnull TPEndpoint endpoint,
                                                @Nonnull Triple triple) {
        return new ProtoQueryNode(endpoint, CQuery.from(triple));
    }

    protected @Nonnull QueryNode createQN(@Nonnull ProtoQueryNode proto) {
        TPEndpoint ep = proto.getEndpoint();
        CQuery query = proto.getQuery();
        return new QueryNode(ep, query, ep.estimate(query, estimatePolicy));
    }

}
