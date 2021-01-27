package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.federation.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.CollectionUtils;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public abstract class AbstractBitsetConjunctivePlanner implements ConjunctivePlanner {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBitsetConjunctivePlanner.class);

    protected final @Nonnull JoinOrderPlanner joinOrderPlanner;
    protected final @Nonnull InnerCardinalityComputer innerCardComputer;

    protected AbstractBitsetConjunctivePlanner(@Nonnull JoinOrderPlanner joinOrderPlanner,
                                            @Nonnull InnerCardinalityComputer innerCardComputer) {
        this.joinOrderPlanner = joinOrderPlanner;
        this.innerCardComputer = innerCardComputer;
    }

    @Override public @Nonnull Op plan(@Nonnull CQuery query, @Nonnull Collection<Op> inFragments) {
        if (checkEmptyPlan(query, inFragments))
            return new EmptyOp(new QueryOp(query));
        List<Op> fragmentsList = inFragments instanceof List ? (List<Op>)inFragments
                : new ArrayList<>(inFragments);
        RefIndexSet<Op> fragments = groupNodes(fragmentsList);
        BitJoinGraph joinGraph = createJoinGraph(fragments);
        Collection<?> components = findComponents(query, joinGraph);

        int nComponents = components.size();
        if (nComponents == 0) {
            return new EmptyOp(new QueryOp(query));
        } else if (nComponents == 1) {
            IndexSubset<Op> component = componentToSubset(joinGraph.getNodes(),
                                                          components.iterator().next());
            return joinOrderPlanner.plan(joinGraph, component);
        } else {
            List<Bitset> shared = findCommonSubsets(components, joinGraph);
            assert validCommonSubsets(shared);
            ArrayList<Op> plans = new ArrayList<>(nComponents);
            for (IndexSubset<Op> subset : replaceShared(components, shared, joinGraph))
                plans.add(joinOrderPlanner.plan(joinGraph, subset));
            return UnionOp.build(plans);
        }
    }

    protected abstract @Nonnull BitJoinGraph createJoinGraph(RefIndexSet<Op> fragments);

    @VisibleForTesting abstract @Nonnull RefIndexSet<Op> groupNodes(@Nonnull List<Op> nodes);

    protected abstract @Nonnull IndexSubset<Op> componentToSubset(@Nonnull RefIndexSet<Op> nodes,
                                                                  @Nonnull Object component);

    @VisibleForTesting abstract @Nonnull Collection<?> findComponents(@Nonnull CQuery query,
                                                                @Nonnull BitJoinGraph nodes);

    @VisibleForTesting abstract @Nonnull List<Bitset>
    findCommonSubsets(@Nonnull Collection<?> components, @Nonnull BitJoinGraph graph);

    @VisibleForTesting abstract @Nonnull List<IndexSubset<Op>>
    replaceShared(@Nonnull Collection<?> inComponents, @Nonnull List<Bitset> sharedSubsets,
                  @Nonnull BitJoinGraph joinGraph);

    protected boolean checkEmptyPlan(@Nonnull CQuery query, @Nonnull Collection<Op> fragments) {
        assert validInput(query, fragments);
        if (query.isEmpty()) //empty query
            return true;
        if (fragments.isEmpty()) {
            logger.debug("No subqueries (lack of sources?). Query: \"\"\"{}\"\"\"", query);
            return true;
        }
        IndexSet<Triple> all = query.attr().getSet();
        IndexSubset<Triple> matched = CollectionUtils.union(all, fragments, Op::getMatchedTriples);
        if (!matched.containsAll(all)) {
            IndexSubset<Triple> missing = all.fullSubset().minus(matched);
            logger.info("Fragments for query miss triples {} in query {}.", missing, query);
            return true;
        }
        return false;
    }

    @SuppressWarnings("AssertWithSideEffects")
    protected boolean validInput(@Nonnull CQuery query, @Nonnull Collection<Op> fragments) {
        //noinspection AssertWithSideEffects
        assert query.attr().isJoinConnected() : "query is not join-connected!";
        IndexSet<Triple> all = query.attr().matchedTriples();
        assert fragments.stream().allMatch(o -> all.containsAll(o.getMatchedTriples()))
                : "Some fragments match triples not in query";
        for (Op fragment : fragments)
            fragment.assertTreeInvariants();
        assert fragments.stream().filter(UnionOp.class::isInstance)
                .allMatch(o -> o.getChildren().stream().noneMatch(UnionOp.class::isInstance))
                : "Some UnionOp instance have other UnionOp as children";
        assert fragments.stream().filter(UnionOp.class::isInstance)
                .allMatch(o -> o.getChildren().stream().map(Op::getMatchedTriples)
                        .distinct().count() == 1)
                : "Some UnionOp instances have children with different matchedTriples";
        assert fragments.stream().noneMatch(o -> !(o instanceof UnionOp)
                && !(o instanceof EndpointQueryOp))
                : "Some fragments are neither UnionOp nor EndpointQueryOp";

        IndexSet<String> varsUniverse = query.attr().allVarNames().getParent();
        String queryUniverseMsg = "Query var names all have the same universe";
        assert query.attr().tripleVarNames().getParent()       == varsUniverse : queryUniverseMsg;
        assert query.attr().inputVarNames().getParent()        == varsUniverse : queryUniverseMsg;
        assert query.attr().reqInputVarNames().getParent()     == varsUniverse : queryUniverseMsg;
        assert query.attr().optInputVarNames().getParent()     == varsUniverse : queryUniverseMsg;
        assert query.attr().publicVarNames().getParent()       == varsUniverse : queryUniverseMsg;
        assert query.attr().publicTripleVarNames().getParent() == varsUniverse : queryUniverseMsg;

        assert fragments.stream().map(Op::getAllVars).allMatch(IndexSubset.class::isInstance);
        assert sameUniverse(fragments, Op::getAllVars);
        assert sameUniverse(fragments, Op::getResultVars);
        assert sameUniverse(fragments, Op::getStrictResultVars);
        assert sameUniverse(fragments, Op::getRequiredInputVars);
        assert sameUniverse(fragments, Op::getOptionalInputVars);
        assert sameUniverse(fragments, Op::getInputVars);
        assert sameUniverse(fragments, Op::getPublicVars);
        assert sameUniverse(fragments, Op::getMatchedTriples);
        assert sameUniverse(fragments, Op::getCachedMatchedTriples);

        assert sameUniverse(fragments, Op::getAllVars, varsUniverse);
        assert sameUniverse(fragments, Op::getResultVars, varsUniverse);
        assert sameUniverse(fragments, Op::getStrictResultVars, varsUniverse);
        assert sameUniverse(fragments, Op::getRequiredInputVars, varsUniverse);
        assert sameUniverse(fragments, Op::getOptionalInputVars, varsUniverse);
        assert sameUniverse(fragments, Op::getInputVars, varsUniverse);
        assert sameUniverse(fragments, Op::getPublicVars, varsUniverse);

        return true;
    }

    private boolean sameUniverse(@Nonnull Collection<Op> fragments,
                                 @Nonnull Function<Op, Set<?>> getter,
                                 @Nullable IndexSet<?> universe) {
        for (Iterator<Set<?>> it = fragments.stream().map(getter).iterator(); it.hasNext(); ) {
            Set<?> subset = it.next();
            assert subset instanceof IndexSubset;
            IndexSet<?> parent = ((IndexSubset<?>) subset).getParent();
            if (universe == null) universe = parent;
            else assert parent == universe;
        }
        return true;
    }

    private boolean sameUniverse(@Nonnull Collection<Op> fragments,
                                 @Nonnull Function<Op, Set<?>> getter) {
        return sameUniverse(fragments, getter, null);
    }

    protected static boolean validCommonSubsets(@Nonnull List<Bitset> sets) {
        assert new HashSet<>(sets).size() == sets.size() : "Duplicate subsets";
        for (int i = 0, nSets = sets.size(); i < nSets; i++) {
            Bitset previous = sets.get(i);
            for (int j = i+1; j < nSets; j++)
                assert !sets.get(j).intersects(previous) : "subsets intersect";
        }
        assert sets.stream().noneMatch(Bitset::isEmpty) : "Empty subset";
        assert sets.stream().noneMatch(s -> s.cardinality() == 1) : "Singleton subset";
        return true;
    }


    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }
}
