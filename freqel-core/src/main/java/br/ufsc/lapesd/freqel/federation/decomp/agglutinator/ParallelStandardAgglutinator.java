package br.ufsc.lapesd.freqel.federation.decomp.agglutinator;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.concurrent.CommonPoolPlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.BitsetOps;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;

public class ParallelStandardAgglutinator implements Agglutinator {
    private static final Function<Bitset, List<Op>> opListFac = k -> new ArrayList<>();

    private @Nullable MatchingStrategy matchingStrategy;
    private @Nonnull final ArrayBlockingQueue<StandardState> statePool
            = new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors()*2);
    private @Nonnull final PerformanceListener perfListener;
    private @Nonnull final PlanningExecutorService executor;

    @Inject
    public ParallelStandardAgglutinator(@Nonnull PerformanceListener perfListener,
                                        @Nonnull PlanningExecutorService planningExecutorService) {
        this.perfListener = perfListener;
        this.executor = planningExecutorService;
    }

    public ParallelStandardAgglutinator() {
        this(NoOpPerformanceListener.INSTANCE, CommonPoolPlanningExecutorService.INSTANCE);
    }

    @Override public @Nonnull StandardState createState(@Nonnull CQuery query) {
        StandardState state = statePool.poll();
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
        private final List<Bitset> ep2net, ep2ext;
        private final List<Set<CQuery>> ep2exq;
        private final List<Map<CQuery, Set<CQuery>>> ep2exq2alt;
        private final List<CQueryMatch> ep2match;
        private  RefIndexSet<TPEndpoint> epSet;
        private final List<Bitset> tmpTriplesWithAlt;
        private final List<Bitset> tmpTriplesWithoutAlt;
        private final List<List<Bitset>> mergeExclusiveTriples;
        private final List<List<CQuery>> tmpQueries;
        private final Bitset inEG;

        public StandardState(@Nonnull CQuery query) {
            super(query);
            assert matchingStrategy != null;
            this.epSet = matchingStrategy.getEndpoints();
            int nEps = epSet.size(), nTriples = triplesUniverse.size();
            inEG = Bitsets.create(query.size());
            ep2net = new ArrayList<>(nEps);
            ep2ext = new ArrayList<>(nEps);
            ep2exq = new ArrayList<>(nEps);
            ep2match = new ArrayList<>(nEps);
            ep2exq2alt = new ArrayList<>(nEps);
            tmpTriplesWithAlt    = new ArrayList<>(nEps);
            tmpTriplesWithoutAlt = new ArrayList<>(nEps);
            tmpQueries = new ArrayList<>(nEps);
            mergeExclusiveTriples = new ArrayList<>(nEps);
            for (int i = 0; i < nEps; i++) {
                ep2net.add(Bitsets.create(nEps));
                ep2ext.add(Bitsets.create(nEps));
                ep2exq.add(new HashSet<>());
                ep2match.add(null);
                ep2exq2alt.add(new HashMap<>());
                tmpTriplesWithAlt   .add(Bitsets.create(nTriples));
                tmpTriplesWithoutAlt.add(Bitsets.create(nTriples));
                tmpQueries.add(new ArrayList<>());
                mergeExclusiveTriples.add(new ArrayList<>());
            }
        }

        public void reset(@Nonnull CQuery query) {
            setQuery(query);
            assert matchingStrategy != null;
            this.epSet = matchingStrategy.getEndpoints();
            int nEps = epSet.size(), nTriples = triplesUniverse.size();

            assert ep2net.size() == ep2match.size();
            assert ep2net.size() == ep2exq.size();
            assert ep2net.size() == ep2ext.size();
            assert ep2net.size() == tmpQueries.size();
            assert ep2net.size() == mergeExclusiveTriples.size();

            for (int i = ep2ext.size(); i < nEps; i++) { // grow storage if got more eps
                ep2net.add(Bitsets.create(nTriples));
                ep2ext.add(Bitsets.create(nTriples));
                ep2exq.add(new HashSet<>());
                ep2exq2alt.add(new HashMap<>());
                tmpQueries.add(new ArrayList<>());
                mergeExclusiveTriples.add(new ArrayList<>());
                tmpTriplesWithAlt.add(Bitsets.create(nTriples));
                tmpTriplesWithoutAlt.add(Bitsets.create(nTriples));
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
        private void splitNonExclusiveTriplesPhase1() {
            assert ep2ext.size() >= epSet.size() && ep2ext.stream().noneMatch(Objects::isNull);
            inEG.clear();
            // phase 0: build a bitset with all triple in some exclusive group
            for (int i = 0, nEps = epSet.size(); i < nEps; i++) {
                for (CQuery eg : ep2exq.get(i))
                    inEG.or(((IndexSubset<Triple>)eg.attr().getSet()).getBitset());
            }
            executor.parallelFor(0, epSet.size(),
                                 this::splitNonExclusiveTriplesPhase1Endpoint);
        }

        private void splitNonExclusiveTriplesPhase1Endpoint(int epIdx) {
            ep2exq2alt.get(epIdx).clear();
            Bitset ex = ep2ext.get(epIdx);
            ex.assign(ep2net.get(epIdx));
            ex.andNot(inEG);
            int size = epSet.size();
            for (int j = 0      ; j < epIdx; j++) ex.andNot(ep2net.get(j));
            for (int j = epIdx+1; j < size ; j++) ex.andNot(ep2net.get(j));
        }

        private void splitNonExclusiveTriplesPhase2(int epIdx) {
            Bitset triples = ep2ext.get(epIdx);
            ep2net.get(epIdx).andNot(triples);
            if (!triples.isEmpty())
                createEG(epIdx, triples);
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

            Bitset tmpTriplesWithAlt = this.tmpTriplesWithAlt.get(epIdx);
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
                Bitset tmpTriplesWithoutAlt = this.tmpTriplesWithoutAlt.get(epIdx);
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
            return alts == null || alts.size() <= 1;
        }

        /** EGs that do not intersect with any other EG are considered mergeable. That is,
         *  they can be copied into every other non-mergeable EG if that EG has no alternative
         *  rewritings and the MergePolicy annotations (if any) allow that merge. The merged
         *  query will replace the non-mergeable EG. */
        private void mergeExclusiveQueries(int epIdx) {
            Bitset mergeable = tmpTriplesWithAlt.get(epIdx);
            TPEndpoint ep = epSet.get(epIdx);
            Set<CQuery> exqSet = ep2exq.get(epIdx);
            int nQueries = exqSet.size();
            if (nQueries <= 1) {
                return;
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
                return;
            }
            Map<CQuery, Set<CQuery>> exq2alt = ep2exq2alt.get(epIdx);
            List<CQuery> tmpQueries = this.tmpQueries.get(epIdx);
            tmpQueries.clear();
            tmpQueries.addAll(exqSet);
            List<Bitset> mergeExclusiveTriples = this.mergeExclusiveTriples.get(epIdx);
            mergeExclusiveTriples.clear();
            for (CQuery eg : tmpQueries) {
                IndexSubset<Triple> triples = (IndexSubset<Triple>) eg.attr().getSet();
                mergeExclusiveTriples.add(triples.getBitset().copy());
            }
            // find queries that always can be merged into others
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
            // merge the mergeable queries into the non-mergeable ones
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

        private @Nullable CQuery tryMerge(@Nonnull TPEndpoint ep, CQuery a, CQuery b) {
            if (!ep.hasRemoteCapability(Capability.CARTESIAN)) {
                if (!a.attr().tripleVarNames().containsAny(b.attr().tripleVarNames()))
                    return null; // do not generate products if they can't be pushed to the remote
            }
            return MergeHelper.tryMerge(a, b);
        }

        @Override public @Nonnull Collection<Op> takeLeaves() {
            try (TimeSampler ignored = Metrics.AGGLUTINATION_MS.createThreadSampler(perfListener)) {
                splitNonExclusiveTriplesPhase1();
                executor.parallelFor(0, epSet.size(), this::takeLeavesEndpointPreprocess);
                List<Op> nodes = buildNodes();
                assert checkNodesUniverseSets(nodes);
                assert checkLostGroups(nodes);
                assert checkLostTriples(nodes);
                statePool.offer(this);
                return nodes;
            }
        }

        private void takeLeavesEndpointPreprocess(int epIdx) {
            assert ep2ext.size() >= epSet.size() && ep2ext.stream().noneMatch(Objects::isNull);
            splitNonExclusiveTriplesPhase2(epIdx);
            assert checkExclusiveNonExclusiveDisjointness(epIdx);
            mergeExclusiveQueries(epIdx);
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
                for (CQuery alt : alts)
                    dest.add(new EndpointQueryOp(ep, alt));
                return true;
            } else {
                return false;
            }
        }

        /**
         * Checks that no non-exclusive triple in {@link StandardState#ep2net}, when converted
         * into a {@link CQuery} will not already exist in ep2exq for that same endpoint
         */
        private boolean checkExclusiveNonExclusiveDisjointness(int epIdx) {
            Set<CQuery> exclusiveQueries = ep2exq.get(epIdx);
            for (Triple triple : triplesUniverse.subset(ep2net.get(epIdx))) {
                CQuery neq = addUniverse(CQuery.from(triple));
                if (exclusiveQueries.contains(neq))
                    return false; //violation
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
