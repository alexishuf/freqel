package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.priv.InputsBitJoinGraph;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.RawAlignedBitSet;
import br.ufsc.lapesd.freqel.util.bitset.ArrayBitset;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;

import static br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.AbstractStateHelper.NODES;

public class BitsetConjunctivePlanner extends AbstractBitsetConjunctivePlanner  {
    @Inject
    public BitsetConjunctivePlanner(@Nonnull JoinOrderPlanner joinOrderPlanner,
                                    @Nonnull InnerCardinalityComputer innerCardComputer) {
        super(joinOrderPlanner, innerCardComputer);
    }

    @Override protected @Nonnull BitJoinGraph createJoinGraph(RefIndexSet<Op> fragments) {
        return new InputsBitJoinGraph(fragments);
    }

    private @Nonnull Bitset toSignature(@Nonnull Op op) {
        IndexSubset<Triple> triples = (IndexSubset<Triple>) op.getMatchedTriples();
        IndexSubset<String> inVars = (IndexSubset<String>) op.getRequiredInputVars();
        int nTriples = triples.getParent().size();
        int nVars = inVars.getParent().size();

        Bitset sig = Bitsets.createFixed(nTriples + nVars);
        sig.assign(triples.getBitset());
        sig.or(nTriples, inVars.getBitset(), 0, nVars);
        return sig;
    }

    private @Nonnull Op cleanEquivalents(@Nonnull List<Op> list) {
        for (int i = 0, size = list.size(); i < size; i++) {
            Op earlier = list.get(i);
            for (int j = i+1; j < size; j++) {
                int keep = TreeUtils.keepEquivalent(earlier, list.get(j));
                if (keep == 0x2) {
                    list.remove(j--);
                    --size;
                } else if (keep == 0x1) {
                    list.remove(i--);
                    --j;
                    --size;
                }
            }
        }
        return UnionOp.build(list);
    }

    @Override @Nonnull RefIndexSet<Op> groupNodes(@Nonnull List<Op> nodes) {
        if (nodes.size() <= 1)
            return RefIndexSet.fromRefDistinct(nodes);
        Map<Bitset, List<Op>> sig2op = new HashMap<>();
        for (Op op : nodes)
            sig2op.computeIfAbsent(toSignature(op), k -> new ArrayList<>()).add(op);
        RefIndexSet<Op> set = new RefIndexSet<>();
        for (List<Op> list : sig2op.values())
            set.add(cleanEquivalents(list));
        return set;
    }

    @Override
    protected @Nonnull IndexSubset<Op> componentToSubset(@Nonnull RefIndexSet<Op> nodes,
                                                         @Nonnull Object component) {
        return nodes.subset((Bitset)component);
    }

    @Override @Nonnull List<Bitset> findComponents(@Nonnull CQuery query,
                                                   @Nonnull BitJoinGraph graph) {
        int nNodes = graph.size();
        HashSet<Bitset> componentsSet = new HashSet<>(nNodes);
        InputStateHelper helper;
        IndexSet<String> offer = query.attr().varNamesUniverseOffer();
        assert offer != null;
        helper = new InputStateHelper(graph, offer, (IndexSubset<Triple>) query.attr().getSet());

        ArrayDeque<long[]> stack = new ArrayDeque<>(nNodes);
        for (int i = 0; i < nNodes; i++) {
            stack.push(helper.createState(i));
            while (!stack.isEmpty()) {
                long[] state = stack.pop();
                if (helper.isFinal(state)) {
                    assert validComponent(helper.bs, state, graph);
                    componentsSet.add(Bitsets.copyFixed(state, helper.bs.componentBegin(0),
                                                         helper.bs.componentEnd(0)));
                } else {
                    helper.forEachNeighbor(state, stack);
                    helper.bs.dealloc(state);
                }
            }
        }
        for (Bitset component : componentsSet)
            assert validComponent(component, graph);
        ArrayList<Bitset> componentsList = new ArrayList<>(componentsSet);
        removeAlternativeComponents(componentsList, graph.getNodes());
        return componentsList;
    }

    private boolean validComponent(@Nonnull Bitset subset, @Nonnull BitJoinGraph graph) {
        assert subset.length()-1 <= graph.size();
        RawAlignedBitSet bs = new RawAlignedBitSet(graph.size());
        long[] state = bs.alloc();
        long[] tmp = subset.toLongArray();
        System.arraycopy(tmp, 0, state, 0, tmp.length);
        assert RawAlignedBitSet.cardinality(state) == subset.cardinality();

        return validComponent(bs, state, graph);
    }

    private boolean validComponent(@Nonnull RawAlignedBitSet bs, long[] state,
                                   @Nonnull BitJoinGraph graph) {
        Set<String> nonIn = new HashSet<>(), in = new HashSet<>();
        int i = bs.nextSet(state, NODES, 0);
        for (; i >=0; i = bs.nextSet(state, NODES, i+1)) {
            nonIn.addAll(graph.getNodes().get(i).getStrictResultVars());
            in.addAll(graph.getNodes().get(i).getRequiredInputVars());
        }
        if (!nonIn.containsAll(in))
            return false;

        IndexSubset<Op> subset = graph.getNodes().subset(bs.asBitset(state, NODES));
        Op plan = joinOrderPlanner.plan(graph, subset);
        return !(plan instanceof EmptyOp);
    }

    @Override
    @Nonnull List<Bitset> findCommonSubsets(@Nonnull Collection<?> componentsColl,
                                            @Nonnull BitJoinGraph graph) {
        //noinspection unchecked
        List<Bitset> comps = componentsColl instanceof List ? (List<Bitset>)componentsColl
                                : new ArrayList<>((Collection<Bitset>) componentsColl);
        List<Bitset> results = new ArrayList<>(comps.size());
        Intersection shared = new Intersection(), notNovel = new Intersection();
        int nNodes = graph.size();
        Bitset sharedRem = Bitsets.createFixed(nNodes), oldRem = Bitsets.createFixed(nNodes);
        for (int i = 0, nComponents = comps.size(); i < nComponents; i++) {
            for (int j = i+1; j < nComponents; j++) {
                if (!shared.intersect(comps.get(i), comps.get(j)) || !isConnected(shared, graph))
                    continue;
                int sharedCard = shared.cardinality();
                if ( sharedCard <= 1) continue;
                if (!isConnected(shared, graph)) continue;
                for (Bitset oldResult : results) {
                    if (sharedCard <= 1 || oldResult.equals(shared)) {
                        shared.clear(); // avoid further call to cardinality()
                        break;
                    }
                    if (!notNovel.intersect(shared, oldResult))
                        continue;

                    // shared & oldResult != 0. Must remove notNovel from one of them
                    oldRem.assign(oldResult);
                    oldRem.andNot(notNovel);
                    sharedRem.assign(shared);
                    sharedRem.andNot(notNovel);
                    boolean canReduceOld = isConnected(oldRem, graph);
                    boolean canReduceShared = isConnected(sharedRem, graph);
                    int oldCard = oldResult.cardinality(), notNovelCard = notNovel.cardinality();
                    if (canReduceShared && (oldCard >= sharedCard || !canReduceOld)) {
                        shared.assign(sharedRem); // reduce shared, keep old
                        assert sharedCard-notNovelCard == shared.cardinality();
                        if ((sharedCard -= notNovelCard) == 1)
                            shared.clear(); // drop faster if singleton
                    } else if (canReduceOld) {
                        oldResult.assign(oldRem); //reduce old, keep shared
                        assert oldCard - notNovelCard == oldResult.cardinality();
                        if (oldCard - notNovelCard == 1)
                            oldResult.clear(); //drop faster if singleton
                    } else { //cannot reduce, keep largest
                        if (sharedCard <= oldCard) shared.clear();
                        else                       oldResult.clear();
                    }
                }
                assert sharedCard == shared.cardinality();
                if (sharedCard > 1)
                    results.add(shared.take());
            }
        }

        ArrayList<Bitset> cleanResults = new ArrayList<>(results.size());
        for (Bitset set : results) {
            if (!set.isEmpty()) cleanResults.add(set);
        }
        return cleanResults;
    }

    private static class Intersection extends ArrayBitset {
        public boolean intersect(@Nonnull Bitset a, @Nonnull Bitset b) {
            clear();
            or(a);
            and(b);
            return !isEmpty();
        }
        public @Nonnull Bitset take() {
            Bitset copy = copy();
            clear();
            return copy;
        }
    }

    private boolean isConnected(@Nonnull Bitset subset, @Nonnull BitJoinGraph graph) {
        IndexSubset<Op> ss = graph.getNodes().subset(subset);
        if (ss.isEmpty())
            return true;
        IndexSubset<Triple> queryTriples = null;
        for (Op op : ss) {
            if (queryTriples == null)
                queryTriples = ((IndexSubset<Triple>)op.getMatchedTriples()).copy();
            else
                queryTriples.addAll(op.getMatchedTriples());
        }
        IndexSet<String> vars = ((IndexSubset<String>)ss.iterator().next().getAllVars()).getParent();
        InputStateHelper helper = new InputStateHelper(graph, vars, queryTriples);

        ArrayDeque<long[]> stack = new ArrayDeque<>(subset.cardinality()*2);
        stack.push(helper.createState(subset.nextSetBit(0)));
        while (!stack.isEmpty()) {
            long[] state = stack.pop();
            if (helper.isFinal(state)) {
                assert validComponent(helper.bs, state, graph);
                return true;
            } else {
                helper.forEachNeighbor(state, i -> {
                    if (subset.get(i)) {
                        long[] next = helper.addNode(state, i);
                        if (next != null) stack.push(next);
                    }
                });
            }
        }
        return false;
    }

    @Override
    @Nonnull List<IndexSubset<Op>> replaceShared(@Nonnull Collection<?> inComponents,
                                                 @Nonnull List<Bitset> sharedSubsets,
                                                 @Nonnull BitJoinGraph joinGraph) {
        RefIndexSet<Op> nodes = joinGraph.getNodes();
        List<IndexSubset<Op>> components = new ArrayList<>(inComponents.size());
        for (Object inComponent : inComponents)
            components.add(nodes.subset((Bitset) inComponent));

        for (Bitset shared : sharedSubsets) {
            Op plan = joinOrderPlanner.plan(joinGraph, nodes.subset(shared));
            int planIdx = nodes.size();
            boolean changed = nodes.add(plan);
            assert changed;
            assert nodes.indexOf(plan) == planIdx;
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
        return components; //now with shared subsets replaced
    }

    private void removeAlternativeComponents(@Nonnull List<Bitset> components,
                                             @Nonnull List<Op> nodes) {
        assert components.stream().noneMatch(Objects::isNull)
                && components.stream().noneMatch(Bitset::isEmpty);
        int nComponents = components.size(), firstElimination = Integer.MAX_VALUE;
        outer_loop:
        for (int i = 0; i < nComponents; i++) {
            Bitset outer = components.get(i);
            if (outer != null) {
                for (int j = i + 1; j < nComponents; j++) {
                    Bitset inner = components.get(j);
                    if (inner != null) {
                        int best = chooseComponent(outer, inner, nodes);
                        if (best == 0) {
                            firstElimination = Math.min(firstElimination, j);
                            components.set(j, null);
                        } else if (best == 1) {
                            firstElimination = Math.min(firstElimination, i);
                            components.set(i, null);
                            continue outer_loop;
                        }
                    }
                }
            }
        }
        if (firstElimination != Integer.MAX_VALUE) {
            for (int i = nComponents-1; i >= firstElimination; i--) {
                if (components.get(i) == null) components.remove(i);
            }
        }
        assert components.stream().noneMatch(Objects::isNull);
    }

    private int chooseComponent(@Nonnull Bitset a, @Nonnull Bitset b, @Nonnull List<Op> nodes) {
        int aScore = 0, bScore = 0;
        if (a.cardinality() != b.cardinality())
            return -1;
        for (int i = a.nextSetBit(0); i >= 0; i = a.nextSetBit(i+1)) {
            boolean hasMatch = b.get(i); // if same node is already in b, do not iterate
            for (int j = b.nextSetBit(0); !hasMatch && j >= 0; j = b.nextSetBit(j+1)) {
                Op aNode = nodes.get(i), bNode = nodes.get(j);
                if (aNode.getMatchedTriples().equals(bNode.getMatchedTriples())) {
                    hasMatch = true;
                    int bits = TreeUtils.keepEquivalent(aNode, bNode);
                    if (bits == 0x3)
                        return -1; //distinct nodes, components are not equivalent
                    aScore += (bits & 0x2) >> 1;
                    bScore +=  bits & 0x1      ;
                }
            }
            if (!hasMatch)
                return -1; // node has no equivalent, thus both components must remain
        }
        return aScore >= bScore ? 0 : 1;
    }
}
