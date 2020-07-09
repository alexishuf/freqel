package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.InputAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.stream.Collectors.toList;

public abstract class SourcesListAbstractDecomposer implements DecompositionStrategy {
    private static final Logger logger
            = LoggerFactory.getLogger(SourcesListAbstractDecomposer.class);
    protected final @Nonnull List<Source> sources = new ArrayList<>();
    protected final @Nonnull Planner planner;
    protected final @Nonnull PerformanceListener performance;

    public SourcesListAbstractDecomposer(@Nonnull Planner planner,
                                         @Nonnull PerformanceListener performance) {
        this.planner = planner;
        this.performance = performance;
    }

    @Override
    public void addSource(@Nonnull Source source) {
        sources.add(source);
    }

    @Override
    public @Nonnull ImmutableCollection<Source> getSources() {
        return ImmutableList.copyOf(sources);
    }

    @Override
    public @Nonnull PlanNode decompose(@Nonnull CQuery query) {
        try (TimeSampler ignored = Metrics.PLAN_MS.createThreadSampler(performance)) {
            FilterAssigner p = new FilterAssigner(query);
            Collection<PlanNode> leaves = decomposeIntoLeaves(query, p);

            performance.sample(Metrics.SOURCES_COUNT, countEndpoints(leaves));
            PlanNode plan = planner.plan(query, leaves);
            p.placeBottommost(plan);
            return plan;
        }
    }

    private int countEndpoints(@Nonnull Collection<PlanNode> leaves) {
        Set<TPEndpoint> endpoints = new HashSet<>(Math.max(10, leaves.size()));
        for (PlanNode leaf : leaves) {
            if (leaf instanceof QueryNode)
                endpoints.add(((QueryNode) leaf).getEndpoint());
            else if (leaf instanceof MultiQueryNode) {
                for (PlanNode child : leaf.getChildren()) {
                    if (child instanceof QueryNode)
                        endpoints.add(((QueryNode) child).getEndpoint());
                    else
                        assert false : "Expected all leaves to be QueryNodes";
                }
            }
        }
        return endpoints.size();
    }

    @Override
    public @Nonnull List<PlanNode> decomposeIntoLeaves(@Nonnull CQuery query) {
        return decomposeIntoLeaves(query, new FilterAssigner(query));
    }

    public @Nonnull List<PlanNode> decomposeIntoLeaves(@Nonnull CQuery query,
                                                        @Nonnull FilterAssigner placement) {
        List<ProtoQueryNode> list = decomposeIntoProtoQNs(query);
        list = minimizeQueryNodes(query, list);
        return placement.placeFiltersOnLeaves(list);
    }

    protected static class Signature {
        final @Nonnull TPEndpoint endpoint;
        final @Nonnull BitSet triples;
        final @Nonnull BitSet inputs;
        int hash = 0;

        public Signature(@Nonnull ProtoQueryNode qn, @Nonnull IndexedSet<Triple> allTriples,
                         @Nonnull IndexedSet<Var> allVars) {
            this.endpoint = qn.getEndpoint();
            this.triples = allTriples.subset(qn.getMatchedQuery()).getBitSet();
            IndexedSubset<Var> inputsSubset = allVars.emptySubset();
            qn.getMatchedQuery().forEachTermAnnotation(InputAnnotation.class, (t, a) -> {
                if (t.isVar()) inputsSubset.add(t.asVar());
            });
            this.inputs = inputsSubset.getBitSet();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Signature)) return false;
            Signature signature = (Signature) o;
            return hashCode() == signature.hashCode() &&
                    endpoint.equals(signature.endpoint) &&
                    triples.equals(signature.triples) &&
                    inputs.equals(signature.inputs);
        }

        @Override
        public int hashCode() {
            if (hash == 0)
                hash = Objects.hash(endpoint, triples, inputs);
            return hash;
        }

        @Override
        public String toString() {
            return String.format("Signature{\n" +
                    "  ins=%s,\n" +
                    "  eps=%s,\n" +
                    "  triples=%s\n}", inputs, endpoint, triples);
        }
    }

    protected @Nonnull List<ProtoQueryNode>
    minimizeQueryNodes(@Nonnull CQuery query, @Nonnull List<ProtoQueryNode> nodes) {
        SetMultimap<Signature, ProtoQueryNode> sig2pn = HashMultimap.create();
        nodes.forEach(pn -> sig2pn.put(new Signature(pn, query.getSet(), query.getVars()), pn));

        List<ProtoQueryNode> list = sig2pn.keySet().stream()
                .map(s -> sig2pn.get(s).iterator().next()).collect(toList());
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
}
