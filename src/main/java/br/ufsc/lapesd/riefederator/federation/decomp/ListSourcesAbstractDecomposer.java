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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.stream.Collectors.toList;

public abstract class ListSourcesAbstractDecomposer implements DecompositionStrategy {
    private static final Logger logger
            = LoggerFactory.getLogger(ListSourcesAbstractDecomposer.class);
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
        List<QueryNode> queryNodes = placement.placeFiltersOnLeaves(list);
        return minimizeQueryNodes(queryNodes);
    }

    private static class Signature {
        final @Nonnull TPEndpoint endpoint;
        final @Nonnull Set<Triple> triples;
        final @Nonnull Set<String> inputs;
        int hash = 0;

        public Signature(@Nonnull QueryNode qn) {
            this.endpoint = qn.getEndpoint();
            this.triples = qn.getMatchedTriples();
            this.inputs = qn.getInputVars();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Signature)) return false;
            Signature signature = (Signature) o;
            return endpoint.equals(signature.endpoint) &&
                    triples.equals(signature.triples) &&
                    inputs.equals(signature.inputs);
        }

        @Override
        public int hashCode() {
            if (this.hash == 0)
                this.hash = Objects.hash(endpoint, triples, inputs);
            return this.hash;
        }

        @Override
        public String toString() {
            return String.format("Signature{\n" +
                    "  ins=%s,\n" +
                    "  ep=%s,\n" +
                    "  triples=%s\n}", inputs, endpoint, triples);
        }
    }

    protected @Nonnull List<QueryNode> minimizeQueryNodes(@Nonnull List<QueryNode> nodes) {
        SetMultimap<Signature, QueryNode> sig2qn = HashMultimap.create();
        nodes.forEach(qn -> sig2qn.put(new Signature(qn), qn));

        List<QueryNode> list = sig2qn.keySet().stream().map(s -> sig2qn.get(s).iterator().next())
                                              .collect(toList());
        if (list.size() < nodes.size()) {
            logger.debug("Discarded {} nodes due to duplicate  endpoint/triple/inputs signatures",
                         nodes.size() - list.size());
        }
        return list;
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
