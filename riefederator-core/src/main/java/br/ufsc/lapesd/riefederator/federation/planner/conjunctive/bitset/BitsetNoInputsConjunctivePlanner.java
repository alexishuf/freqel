package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.RawAlignedBitSet;
import br.ufsc.lapesd.riefederator.util.bitset.Bitsets;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;

import static br.ufsc.lapesd.riefederator.util.RawAlignedBitSet.andNot;
import static br.ufsc.lapesd.riefederator.util.RawAlignedBitSet.cardinality;

public class BitsetNoInputsConjunctivePlanner extends AbstractBitsetConjunctivePlanner {
    private boolean extractCommonSubsets = true;

    @Inject
    public BitsetNoInputsConjunctivePlanner(@Nonnull JoinOrderPlanner joinOrderPlanner,
                                            @Nonnull InnerCardinalityComputer innerCardComputer) {
        super(joinOrderPlanner, innerCardComputer);
    }

    public void setExtractCommonSubsets(boolean extractCommonSubsets) {
        this.extractCommonSubsets = extractCommonSubsets;
    }

    @Override
    protected boolean validInput(@Nonnull CQuery query, @Nonnull Collection<Op> fragments) {
        super.validInput(query, fragments);
        assert fragments.stream().noneMatch(Op::hasInputs) : "Some fragments have inputs";
        return true;
    }

    @Override protected @Nonnull BitJoinGraph createJoinGraph(RefIndexSet<Op> fragments) {
        return new BitJoinGraph(fragments);
    }

    @Override @Nonnull List<IndexSubset<Op>>
    replaceShared(@Nonnull Collection<?> inComponents, @Nonnull List<Bitset> sharedSubsets,
                  @Nonnull BitJoinGraph joinGraph) {
        RefIndexSet<Op> nodes = joinGraph.getNodes();
        List<IndexSubset<Op>> components = new ArrayList<>(inComponents.size());
        for (Object inComponent : inComponents)
            components.add(nodes.subset(Bitsets.wrap((long[]) inComponent)));

        if (extractCommonSubsets) {
            for (Bitset shared : sharedSubsets) {
                Op plan = joinOrderPlanner.plan(joinGraph, nodes.subset(shared));
                int planIdx = nodes.size();
                boolean changed = nodes.add(plan);
                assert changed;
                Bitset sharedCopy = shared.copy();
                for (IndexSubset<Op> component : components) {
                    sharedCopy.or(shared); // restore bits removed by andNot below
                    Bitset componentBitset = component.getBitset();
                    sharedCopy.andNot(componentBitset);
                    if (sharedCopy.isEmpty()) { // component.containsAll(shared)
                        componentBitset.andNot(shared); // remove shared
                        componentBitset.set(planIdx);   // add the plan
                    }
                }
            }
            joinGraph.notifyAddedNodes(); //compute new JoinInfos
        }
        return components; //now with shared subsets replaced
    }

    @SuppressWarnings("unchecked") @VisibleForTesting @Override
    @Nonnull List<Bitset> findCommonSubsets(@Nonnull Collection<?> components,
                                            @Nonnull BitJoinGraph graph) {
        if (extractCommonSubsets) {
            int nNodes = graph.size();
            if (nNodes <= 64)
                return findCommonSubsetsScalar((List<long[]>) components);
            return findCommonSubsetsArray((List<long[]>) components, nNodes);
        } else {
            return Collections.emptyList();
        }
    }

    @VisibleForTesting
    static @Nonnull List<Bitset> findCommonSubsetsScalar(@Nonnull List<long[]> states) {
        int nStates = states.size(), nResults = 0;
        if (nStates <= 1)
            return Collections.emptyList();
        long shared, notNovel;
        long[] results = new long[nStates*nStates];

        for (int i = 0; i < nStates; i++) {
            long[] state = states.get(i);
            for (int j = i+1; j < nStates; j++) {
                if ((shared = state[0] & states.get(j)[0]) == 0) continue;
                for (int k = 0; k < nResults; k++) {
                    if ((notNovel = shared & results[k]) == 0) continue;
                    if (shared == results[k]) {
                        shared = 0;
                        break;
                    }
                    // remove intersection from smallest subset
                    if (Long.bitCount(results[k]) > Long.bitCount(shared))  shared &= ~notNovel;
                    else                                                    results[k] &= ~notNovel;
                }
                if (shared != 0 && Long.bitCount(shared) > 1)
                    results[nResults++] = shared; //store unique shared subset
            }
        }
        long[] tmp = new long[1];
        List<Bitset> bsList = new ArrayList<>(nResults);
        for (int i = 0; i < nResults; i++) {
            if (results[i] != 0) {
                tmp[0] = results[i];
                bsList.add(Bitsets.wrap(tmp));
            }
        }
        assert validCommonSubsets(bsList);
        return bsList;
    }

    @VisibleForTesting
    static @Nonnull List<Bitset> findCommonSubsetsArray(@Nonnull List<long[]> states, int nNodes) {
        int nStates = states.size();
        if (nStates <= 1)
            return Collections.emptyList();
        RawAlignedBitSet rbs = new RawAlignedBitSet(nNodes);
        long[] shared = rbs.alloc(), notNovel = rbs.alloc();
        List<long[]> results = new ArrayList<>();
        for (int i = 0; i < nStates; i++) {
            long[] outer = states.get(i);
            for (int j = i+1; j < nStates; j++) {
                if (!rbs.and(0, shared, outer, states.get(j))) continue;
                for (long[] result : results) {
                    if (!RawAlignedBitSet.and(notNovel, shared, result)) continue;
                    if (rbs.equals(shared, result)) {
                        RawAlignedBitSet.clear(shared);
                        break;
                    }
                    if (cardinality(shared) > cardinality(result)) andNot(shared, notNovel);
                    else                                           andNot(result, notNovel);
                }
                if (cardinality(shared) > 1)
                    results.add(shared);
            }
        }
        List<Bitset> bitSets = new ArrayList<>(results.size());
        for (long[] result : results) {
            if (!RawAlignedBitSet.isEmpty(result))
                bitSets.add(Bitsets.wrap(result));
        }
        assert validCommonSubsets(bitSets);
        return bitSets;
    }

    @VisibleForTesting @Override
    @Nonnull List<?> findComponents(@Nonnull CQuery query, @Nonnull BitJoinGraph nodes) {
        int nNodes = nodes.size();
        ArrayList<long[]> result = new ArrayList<>(nNodes);
        NoInputStateHelper helper;
        helper = new NoInputStateHelper(nodes, query.attr().varNamesUniverseOffer(),
                                        (IndexSubset<Triple>) query.attr().getSet());
        long[] included = helper.createState();
        ArrayDeque<long[]> stack = new ArrayDeque<>(nNodes);
        for (int i = 0; i < nNodes; i++) {
            stack.push(helper.createState(i));
            while (!stack.isEmpty()) {
                long[] state = stack.pop();
                if (helper.isFinal(state)) {
                    if (!helper.bs.containsAll(0, included, state)) {
                        helper.bs.or(0, included, state);
                        helper.bs.clearFrom(state, 1);
                        result.add(state);
                    }
                } else {
                    helper.forEachNeighbor(state, stack);
                    helper.bs.dealloc(state);
                }
            }
        }
        return result;
    }

    @Override
    protected @Nonnull IndexSubset<Op> componentToSubset(@Nonnull RefIndexSet<Op> nodes,
                                                         @Nonnull Object component) {
        return nodes.subset(Bitsets.wrap((long[]) component));
    }

    @VisibleForTesting @Override @Nonnull RefIndexSet<Op> groupNodes(@Nonnull List<Op> nodes) {
        if (nodes.size() <= 1)
            return RefIndexSet.fromRefDistinct(nodes);
        long[] sorted = new long[nodes.size()];
        int nextIdx = 0;
        for (Op op : nodes) {
            long hash = ((IndexSubset<Triple>) op.getMatchedTriples()).getBitset().hashCode();
            sorted[nextIdx] = (hash << 32) | (long)nextIdx;
            ++nextIdx;
        }
        Arrays.sort(sorted);

        RefIndexSet<Op> result = new RefIndexSet<>(nodes.size());
        int fromHash = (int)(sorted[0] >>> 32), from = 0;
        Set<Triple> fromMatched = nodes.get((int)sorted[0]).getMatchedTriples();
        for (int i = 1; i < sorted.length; i++) {
            int hash = (int) (sorted[i] >>> 32), pos = (int)sorted[i];
            Set<Triple> matched = nodes.get(pos).getMatchedTriples();
            if (hash != fromHash || !fromMatched.equals(matched)) {
                merge(nodes, sorted, result, from, i);
                from = i; // start new sequence from i
                fromHash = hash;
                fromMatched = matched;
            }
        }
        merge(nodes, sorted, result, from, sorted.length);
        return result;
    }

    private static void merge(@Nonnull List<Op> nodes, long[] sorted, RefIndexSet<Op> result,
                              int begin, int end) {
        assert begin < end;
        if (end - begin > 1) { //there is a merge-able sequence of nodes
            ArrayList<Op> list = new ArrayList<>(end - begin);
            Bitset ignore = Bitsets.createFixed(end-begin);
            for (int i = begin; i < end; i++) {
                if (ignore.get(i-begin)) continue;
                Op earlier = nodes.get((int) sorted[i]);
                for (int j = i + 1; j < end; j++) {
                    Op later = nodes.get((int) sorted[j]);
                    if (!ignore.get(j-begin) &&  TreeUtils.areEquivalent(earlier, later))
                        ignore.set(j - begin);
                }
                list.add(earlier);
            }
            result.add(UnionOp.build(list));
        } else { // failed to start a sequence, save last Op
            result.add(nodes.get((int) sorted[end-1]));
        }
    }
}
