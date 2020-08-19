package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.InputAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Limit;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
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

public abstract class SourcesListAbstractDecomposer implements DecompositionStrategy {
    private static final Logger logger
            = LoggerFactory.getLogger(SourcesListAbstractDecomposer.class);
    protected final @Nonnull List<Source> sources = new ArrayList<>();
    protected final @Nonnull Planner planner;
    protected final @Nonnull PerformanceListener performance;

    protected SourcesListAbstractDecomposer(@Nonnull Planner planner,
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
    public @Nonnull Op decompose(@Nonnull CQuery query) {
        try (TimeSampler ignored = Metrics.PLAN_MS.createThreadSampler(performance)) {
            FilterAssigner p = new FilterAssigner(query);
            Collection<Op> leaves = decomposeIntoLeaves(query, p);

            performance.sample(Metrics.SOURCES_COUNT, countEndpoints(leaves));
            Op plan = planner.plan(query, leaves);
            p.placeBottommost(plan);
            Distinct distinct = query.getModifiers().distinct();
            Optional optional = query.getModifiers().optional();
            Limit limit = query.getModifiers().limit();
            if (distinct != null)
                plan.modifiers().add(distinct);
            if (optional != null)
                plan.modifiers().add(optional);
            if (limit != null)
                plan.modifiers().add(limit);
            return plan;
        }
    }

    private int countEndpoints(@Nonnull Collection<Op> leaves) {
        Set<TPEndpoint> endpoints = new HashSet<>(Math.max(10, leaves.size()));
        for (Op leaf : leaves) {
            if (leaf instanceof EndpointQueryOp)
                endpoints.add(((EndpointQueryOp) leaf).getEndpoint());
            else if (leaf instanceof UnionOp) {
                for (Op child : leaf.getChildren()) {
                    if (child instanceof EndpointQueryOp)
                        endpoints.add(((EndpointQueryOp) child).getEndpoint());
                    else
                        assert false : "Expected all leaves to be QueryNodes";
                }
            }
        }
        return endpoints.size();
    }

    @Override
    public @Nonnull List<Op> decomposeIntoLeaves(@Nonnull CQuery query) {
        return decomposeIntoLeaves(query, new FilterAssigner(query));
    }

    public @Nonnull List<Op> decomposeIntoLeaves(@Nonnull CQuery query,
                                                 @Nonnull FilterAssigner placement) {
        List<ProtoQueryOp> list = decomposeIntoProtoQNs(query);
        list = minimizeQueryNodes(query, list);
        return placement.placeFiltersOnLeaves(list);
    }

    protected static class Signature {
        final @Nonnull TPEndpoint endpoint;
        final @Nonnull BitSet triples;
        final @Nonnull BitSet inputs;
        int hash = 0;

        public Signature(@Nonnull ProtoQueryOp qn, @Nonnull IndexedSet<Triple> allTriples,
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

    protected @Nonnull SetMultimap<Signature, ProtoQueryOp>
    groupAndDedup(@Nonnull CQuery query, @Nonnull Collection<ProtoQueryOp> in) {
        SetMultimap<Signature, ProtoQueryOp> sig2pn = HashMultimap.create();
        IndexedSet<Triple> triples = query.attr().getSet();
        IndexedSet<Var> vars = query.attr().allVars();
        for (ProtoQueryOp pn : in) sig2pn.put(new Signature(pn, triples, vars), pn);

        Set<CQuery> tmp = new HashSet<>();
        for (Signature sig : sig2pn.keySet()) {
            Set<ProtoQueryOp> set = sig2pn.get(sig);
            if (set.size() > 1) {
                tmp.clear();
                set.removeIf(protoQueryNode -> !tmp.add(protoQueryNode.getMatchedQuery()));
            }
        }
        return sig2pn;
    }

    protected @Nonnull List<ProtoQueryOp>
    minimizeQueryNodes(@Nonnull CQuery query, @Nonnull List<ProtoQueryOp> nodes) {
        SetMultimap<Signature, ProtoQueryOp> sig2pn = groupAndDedup(query, nodes);
        List<ProtoQueryOp> list = new ArrayList<>(sig2pn.values());
        if (list.size() < nodes.size()) {
            logger.debug("Discarded {} nodes due to duplicate  endpoint/triple/inputs signatures",
                         nodes.size() - list.size());
        }
        return list;
    }

    /**
     * Decompose the query into a list of {@link ProtoQueryOp}.
     * The result of this method will undergo filter distribution at a later stage.
     *
     * @param query The query to decompose.
     * @return a List of non-null {@link ProtoQueryOp} instances. All triples in query must
     *         appear in at least one of the {@link ProtoQueryOp}s.
     */
    protected abstract @Nonnull List<ProtoQueryOp> decomposeIntoProtoQNs(@Nonnull CQuery query);
}
