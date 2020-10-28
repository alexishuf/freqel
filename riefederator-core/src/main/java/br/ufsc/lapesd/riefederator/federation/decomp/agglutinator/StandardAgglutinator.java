package br.ufsc.lapesd.riefederator.federation.decomp.agglutinator;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.bitset.Bitsets;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.BitsetOps;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

public class StandardAgglutinator implements Agglutinator {
    private static final Function<Bitset, List<Op>> opListFac = k -> new ArrayList<>();

    private @Nullable MatchingStrategy matchingStrategy;
    private @Nonnull final AtomicReference<StandardState> lastState = new AtomicReference<>();
    private @Nonnull final PerformanceListener perfListener;

    @Inject
    public StandardAgglutinator(@Nonnull PerformanceListener perfListener) {
        this.perfListener = perfListener;
    }

    public StandardAgglutinator() {
        this(NoOpPerformanceListener.INSTANCE);
    }

    @Override public @Nonnull StandardState createState(@Nonnull CQuery query) {
        StandardState state = lastState.getAndSet(null);
        if (state != null) {
            state.reset(query);
            return state;
        }
        return new StandardState(query);
    }

    @Override public void setMatchingStrategy(@Nonnull MatchingStrategy strategy) {
        matchingStrategy = strategy;
    }

    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }

    public class StandardState extends AbstractAgglutinatorState {


        private List<Bitset> ep2net, ep2ext;
        private List<Set<CQuery>> ep2exq;
        private List<Map<CQuery, Set<CQuery>>> ep2exq2alt;
        private List<CQueryMatch> ep2match;
        private RefIndexSet<TPEndpoint> epSet;
        private Bitset tmpTriplesWithAlt;
        private Bitset tmpTriplesWithoutAlt;
        private List<Bitset> mergeExclusiveTriples;
        private List<CQuery> tmpQueries;

        public StandardState(@Nonnull CQuery query) {
            super(query);
            this.epSet = matchingStrategy.getEndpointsSet();
            int epSetSize = epSet.size();
            ep2net = new ArrayList<>(epSetSize);
            ep2ext = new ArrayList<>(epSetSize);
            ep2exq = new ArrayList<>(epSetSize);
            ep2match = new ArrayList<>(epSetSize);
            ep2exq2alt = new ArrayList<>(epSetSize);
            for (int i = 0; i < epSetSize; i++) {
                ep2net.add(Bitsets.create(epSetSize));
                ep2ext.add(Bitsets.create(epSetSize));
                ep2match.add(null);
                ep2exq.add(new HashSet<>());
                ep2exq2alt.add(new HashMap<>());
            }
            tmpTriplesWithAlt    = Bitsets.createFixed(triplesUniverse.size());
            tmpTriplesWithoutAlt = Bitsets.createFixed(triplesUniverse.size());
            tmpQueries = new ArrayList<>();
            mergeExclusiveTriples = new ArrayList<>();
        }

        public void reset(@Nonnull CQuery query) {
            setQuery(query);
            this.epSet = matchingStrategy.getEndpointsSet();
            assert ep2net.size() == ep2match.size();
            int nTriples = triplesUniverse.size();
            for (int i = ep2ext.size(), size = epSet.size(); i < size; i++) {
                ep2net.add(Bitsets.create(nTriples));
                ep2ext.add(Bitsets.create(nTriples));
                ep2exq.add(new HashSet<>());
                ep2exq2alt.add(new HashMap<>());
            }
            if (tmpTriplesWithAlt.size() < nTriples) {
                tmpTriplesWithAlt    = Bitsets.createFixed(nTriples);
                tmpTriplesWithoutAlt = Bitsets.createFixed(nTriples);
            }
        }

        @Override public void addMatch(@Nonnull TPEndpoint ep, @Nonnull CQueryMatch match) {
            int epIdx = epSet.indexOf(ep);
            ep2match.set(epIdx, match);
            Bitset neTriples = ep2net.get(epIdx);
            neTriples.clear();
            BitsetOps.union(triplesUniverse, neTriples, match.getNonExclusiveRelevant());

            Set<CQuery> queries = ep2exq.get(epIdx);
            queries.clear();
            for (CQuery eg : match.getKnownExclusiveGroups())
                queries.add(addUniverse(eg));
        }

        /**
         * Split {@link StandardState#ep2net} into itself and {@link StandardState#ep2ext}.
         * For any given i, ep2ne.get(i) and ep2ex.get(i) will not intersect and no two
         * bitsets in ep2ex will intersect.
         *
         * All identified exclusive groups (including single-triple EGs) will be saved
         * into {@link StandardState#ep2exq}.
         */
        private void splitNonExclusiveTriples() {
            assert ep2ext.size() >= epSet.size() && ep2ext.stream().noneMatch(Objects::isNull);
            // phase 1: find exclusive groups
            for (int i = 0, size = epSet.size(); i < size; i++) {
                Bitset ex = ep2ext.get(i);
                ex.assign(ep2net.get(i));
                for (int j = 0  ; j < i   ; j++) ex.andNot(ep2net.get(j));
                for (int j = i+1; j < size; j++) ex.andNot(ep2net.get(j));
            }
            // phase 2: remove the exclusive groups from ep2ne
            for (int i = 0, size = epSet.size(); i < size; i++) {
                ep2exq2alt.get(i).clear();
                Bitset triples = ep2ext.get(i);
                ep2net.get(i).andNot(triples);
                if (!triples.isEmpty())
                    createEG(i, triples);
            }

            // sanity:
            assert IntStream.range(0, ep2ext.size()).noneMatch(i ->
                    IntStream.range(0, ep2ext.size())
                             .anyMatch(j -> i != j && ep2ext.get(i).intersects(ep2ext.get(j))));
            assert IntStream.range(0, ep2ext.size())
                    .noneMatch(i -> ep2net.get(i).intersects(ep2ext.get(i)));
        }

        private void createEG(int epIdx, @Nonnull Bitset triples) {
            Set<CQuery> exclusiveQueries = ep2exq.get(epIdx);
            Map<CQuery, Set<CQuery>> exq2alt = ep2exq2alt.get(epIdx);
            CQueryMatch m = ep2match.get(epIdx);
            CQuery q = toQuery(triples);
            if (!(m instanceof SemanticCQueryMatch)) {
                exclusiveQueries.add(q);
                return;
            }

            tmpTriplesWithAlt.clear();
            SemanticCQueryMatch sm = (SemanticCQueryMatch) m;
            for (int i = triples.nextSetBit(0); i >= 0; i = triples.nextSetBit(i+1)) {
                if (sm.getAlternatives(triplesUniverse.get(i)).size() > 1)
                    tmpTriplesWithAlt.set(i);
            }

            int nTriplesWithAlt = tmpTriplesWithAlt.cardinality();
            if (nTriplesWithAlt == 0) {
                exclusiveQueries.add(q);
            } else if (nTriplesWithAlt == 1) {
                // a single triple has alternatives, uses those to build the
                // alternatives by combining them with the other triples which are fixed
                int tripleWithAltIdx = tmpTriplesWithAlt.nextSetBit(0);
                Triple tripleWithAlt = triplesUniverse.get(tripleWithAltIdx);
                exclusiveQueries.add(q);

                Set<CQuery> alternatives = sm.getAlternatives(tripleWithAlt);
                assert alternatives.size() > 1;
                Bitset fixedTriples = triples.copy();
                fixedTriples.clear(tripleWithAltIdx);
                CQuery fixed = toQuery(fixedTriples);
                Set<CQuery> altQueries = Sets.newHashSetWithExpectedSize(alternatives.size());
                for (CQuery alt : alternatives) {
                    CQuery altQuery = addUniverse(CQuery.merge(alt, fixed));
                    assert altQuery.attr().matchedTriples().equals(q.attr().getSet());
                    altQueries.add(altQuery);
                }
                exq2alt.put(q, altQueries);
            } else {
                // Chaos ensues if we try to to build a single node for the EG
                // (factorial explosion). Build a node for each member that has
                // alternatives and a single node for all that have none
                tmpTriplesWithoutAlt.assign(triples);
                tmpTriplesWithoutAlt.andNot(tmpTriplesWithAlt);
                exclusiveQueries.add(toQuery(tmpTriplesWithoutAlt));

                for (int i = tmpTriplesWithAlt.nextSetBit(0); i >= 0;
                         i = tmpTriplesWithAlt.nextSetBit(i+1)) {
                    Triple triple = triplesUniverse.get(i);
                    CQuery matched = addUniverse(CQuery.from(triple));
                    Set<CQuery> alts = sm.getAlternatives(triple);
                    for (CQuery alt : alts) addUniverse(alt);
                    exq2alt.put(matched, alts);
                }
            }
        }

        private boolean isMergeCandidate(int epIdx, @Nullable CQuery q) {
            if (q == null) return false;
            if (!q.attr().inputVarNames().isEmpty()) return false;
            CQueryMatch m = ep2match.get(epIdx);
            if (m instanceof SemanticCQueryMatch
                    && ((SemanticCQueryMatch)m).getAlternatives(q).size() > 1) {
                return false;
            }
            Set<CQuery> alts = ep2exq2alt.get(epIdx).get(q);
            if (alts != null && alts.size() > 1)
                return false;
            return true;
        }

        /** EGs that do not intersect with any other EG are considered mergeable. That is,
         *  they can be copied into every other non-mergeable EG if that EG has no alternative
         *  rewritings and the MergePolicy annotations (if any) allow that merge. The merged
         *  query will replace the non-mergeable EG. */
        private void mergeExclusiveQueries() {
            Bitset mergeable = tmpTriplesWithAlt;
            for (int epIdx = 0, nEps = epSet.size(); epIdx < nEps; epIdx++) {
                TPEndpoint ep = epSet.get(epIdx);
                Set<CQuery> exqSet = ep2exq.get(epIdx);
                int nQueries = exqSet.size();
                if (nQueries <= 1) {
                    continue;
                } else if (nQueries == 2) {
                    Iterator<CQuery> it = exqSet.iterator();
                    CQuery a = it.next();
                    CQuery b = it.next();
                    if (isMergeCandidate(epIdx, a) && isMergeCandidate(epIdx, b)) {
                        CQuery merged = tryMerge(ep, a, b);
                        if (merged != null) {
                            exqSet.clear();
                            exqSet.add(merged);
                        }
                    }
                    continue;
                }
                Map<CQuery, Set<CQuery>> exq2alt = ep2exq2alt.get(epIdx);
                tmpQueries.clear();
                tmpQueries.addAll(exqSet);
                mergeExclusiveTriples.clear();
                for (CQuery eg : tmpQueries) {
                    IndexSubset<Triple> triples = (IndexSubset<Triple>) eg.attr().getSet();
                    mergeExclusiveTriples.add(triples.getBitset().copy());
                }
                mergeable.clear();
                for (int i = 0; i < nQueries; i++) {
                    if (!isMergeCandidate(epIdx, tmpQueries.get(i)))
                        mergeable.set(i);
                    Bitset outer = mergeExclusiveTriples.get(i);
                    for (int j = i+1; j < nQueries; j++) {
                        if (outer.intersects(mergeExclusiveTriples.get(j))) {
                            mergeable.set(i);
                            mergeable.set(j);
                        }
                    }
                }
                mergeable.flip(0, nQueries);
                for (int i = mergeable.nextSetBit(0); i >= 0; i = mergeable.nextSetBit(i+1)) {
                    CQuery outer = tmpQueries.get(i);
                    assert isMergeCandidate(epIdx, outer);
                    for (int j = 0; j < nQueries; j++) {
                        CQuery inner = tmpQueries.get(j);
                        if (i != j && !mergeable.get(j) && isMergeCandidate(epIdx, inner)) {
                            CQuery merged = tryMerge(ep, outer, inner);
                            if (merged != null) {
                                exq2alt.remove(inner);
                                exqSet.remove(inner);
                                exqSet.add(merged);
                                tmpQueries.set(j, merged);
                            }
                        }
                    }
                }
                // merge the mergeable queries among themselves
                for (int i = mergeable.nextSetBit(0); i >= 0; i = mergeable.nextSetBit(i+1)) {
                    CQuery outer = tmpQueries.get(i);
                    for (int j = mergeable.nextSetBit(i+1); j >= 0; j = mergeable.nextSetBit(j+1)) {
                        CQuery inner = tmpQueries.get(j);
                        CQuery merged = tryMerge(ep, outer, inner);
                        if (merged != null) {
                            tmpQueries.set(j, merged);
                            exqSet.remove(inner);
                            exqSet.add(merged);
                        }
                    }
                }
            }
        }

        private @Nullable CQuery tryMerge(@Nonnull TPEndpoint ep, CQuery a, CQuery b) {
            if (!ep.hasRemoteCapability(Capability.CARTESIAN)) {
                if (!a.attr().tripleVarNames().containsAny(b.attr().tripleVarNames()))
                    return null; // do not generate products if they can't be pushed to the remote
            }
            return MergeHelper.tryMerge(a, b);
        }

        @Override public @Nonnull Collection<Op> takeLeaves() {
            try (TimeSampler ignored = Metrics.AGGLUTINATION_MS.createThreadSampler(perfListener)) {
                splitNonExclusiveTriples();
                assert checkExclusiveNonExclusiveDisjointness();
                mergeExclusiveQueries();
                List<Op> nodes = buildNodes();
                for (Op node : nodes) {
                    node.offerVarsUniverse(varsUniverse);
                    node.offerTriplesUniverse(triplesUniverse);
                }
                lastState.set(this);
                assert checkNodesUniverseSets(nodes);
                assert checkLostGroups(nodes);
                assert checkLostTriples(nodes);
                return nodes;
            }
        }

        private boolean checkNodesUniverseSets(@Nonnull List<Op> nodes) {
            for (Op node : nodes) {
                assert node.getOfferedTriplesUniverse() == triplesUniverse;
                assert node.getOfferedVarsUniverse() == varsUniverse;
                assert node.getAllVars() instanceof IndexSubset;
                assert ((IndexSubset<String>)node.getAllVars()).getParent() == varsUniverse;
                assert ((IndexSubset<String>)node.getInputVars()).getParent() == varsUniverse;
                assert ((IndexSubset<String>)node.getResultVars()).getParent() == varsUniverse;
                assert ((IndexSubset<Triple>)node.getMatchedTriples()).getParent() == triplesUniverse;
                if (node instanceof UnionOp)
                    checkNodesUniverseSets(node.getChildren());
            }
            return true;
        }

        private List<Op> buildNodes() {
            List<Op> result = new ArrayList<>();
            for (int epIdx = 0, nEps = epSet.size(); epIdx < nEps; epIdx++) {
                TPEndpoint ep = epSet.get(epIdx);
                Map<CQuery, Set<CQuery>> exq2alt = ep2exq2alt.get(epIdx);
                CQueryMatch m = ep2match.get(epIdx);
                SemanticCQueryMatch sm = (m instanceof SemanticCQueryMatch)
                                       ? (SemanticCQueryMatch)m : null;
                for (CQuery eg : ep2exq.get(epIdx)) {
                    if (sm == null || !addAlts(ep, result, eg, sm.getAlternatives(eg))) {
                        if (!addAlts(ep, result, eg, exq2alt.get(eg)))
                            result.add(new EndpointQueryOp(ep, eg));
                    }
                }
                Bitset net = ep2net.get(epIdx);
                for (int i = net.nextSetBit(0); i >= 0; i = net.nextSetBit(i+1)) {
                    Triple triple = triplesUniverse.get(i);
                    CQuery q = addUniverse(CQuery.from(triple));
                    if (sm == null || !addAlts(ep, result, q, sm.getAlternatives(triple)))
                        result.add(new EndpointQueryOp(ep, q));
                }
            }
            return result;
        }

        private boolean addAlts(@Nonnull TPEndpoint ep, @Nonnull List<Op> dest,
                                @Nonnull CQuery matched, @Nullable Collection<CQuery> alts) {
            int size = alts == null ? 0 : alts.size();
            if (size == 1) {
                dest.add(new EndpointQueryOp(ep, matched));
                return true;
            } else if (size > 0)  {
                List<Op> list = new ArrayList<>(size);
                for (CQuery alt : alts)
                    list.add(new EndpointQueryOp(ep, alt));
                dest.add(UnionOp.build(list));
                return true;
            } else {
                return false;
            }
        }

        /**
         * Checks that no non-exclusive triple in {@link StandardState#ep2net}, when converted
         * into a {@link CQuery} will not already exist in ep2exq for that same endpoint
         */
        private boolean checkExclusiveNonExclusiveDisjointness() {
            for (int epIdx = 0; epIdx < epSet.size(); epIdx++) {
                Set<CQuery> exclusiveQueries = ep2exq.get(epIdx);
                for (Triple triple : triplesUniverse.subset(ep2net.get(epIdx))) {
                    CQuery neq = addUniverse(CQuery.from(triple));
                    if (exclusiveQueries.contains(neq))
                        return false; //violation
                }
            }
            return true;
        }

        private boolean containsQuery(@Nonnull Op op, @Nonnull CQuery query) {
            if (op instanceof UnionOp)
                return op.getChildren().stream().anyMatch(c -> containsQuery(c, query));
            assert op instanceof EndpointQueryOp;
            return MergeHelper.isContained(query, ((EndpointQueryOp)op).getQuery());
        }

        private boolean checkLostGroups(@Nonnull List<Op> nodes) {
            for (CQueryMatch match : ep2match) {
                if (match != null) {
                    SemanticCQueryMatch sm = match instanceof SemanticCQueryMatch
                                           ? (SemanticCQueryMatch)match : null;
                    for (CQuery eg : match.getKnownExclusiveGroups()) {
                        if (sm != null && !sm.getAlternatives(eg).isEmpty()) {
                            for (CQuery alt : sm.getAlternatives(eg)) {
                                if (nodes.stream().noneMatch(n -> containsQuery(n, alt)))
                                    return false;
                            }
                        } else {
                            if (nodes.stream().noneMatch(n -> containsQuery(n, eg)))
                                return false;
                        }
                    }
                }
            }
            return true;
        }

        private boolean checkLostTriples(@Nonnull List<Op> nodes) {
            for (CQueryMatch match : ep2match) {
                SemanticCQueryMatch sm = match instanceof SemanticCQueryMatch
                                       ? (SemanticCQueryMatch)match : null;
                if (match != null) {
                    for (Triple triple : match.getNonExclusiveRelevant()) {
                        if (sm != null && !sm.getAlternatives(triple).isEmpty()) {
                            for (CQuery alt : sm.getAlternatives(triple)) {
                                if (nodes.stream().noneMatch(n -> containsQuery(n, alt)))
                                    return false;
                            }
                        } else {
                            CQuery query = CQuery.from(triple);
                            if (nodes.stream().noneMatch(n -> containsQuery(n, query)))
                                return false;
                        }
                    }
                }
            }
            return true;
        }
    }
}
