package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.streamPreOrder;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class BitsetConjunctivePlannerTest {

    public static void addUniverseSets(@Nonnull CQuery query) {
        if (query.attr().triplesUniverseOffer() != null) {
            assertNotNull(query.attr().varNamesUniverseOffer());
            return;
        }
        FullIndexSet<Triple> triples = new FullIndexSet<>(query.size());
        triples.addAll(query);
        triples.addAll(query.attr().matchedTriples());
        IndexSet<String> vars = FullIndexSet.fromDistinctCopy(query.attr().allVarNames());
        query.attr().offerTriplesUniverse(triples);
        query.attr().offerVarNamesUniverse(vars);
    }

    public static void addUniverseSets(@Nonnull Op root) {
        FullIndexSet<Triple> triples = new FullIndexSet<>(32);
        triples.addAll(root.getMatchedTriples());
        streamPreOrder(root).filter(QueryOp.class::isInstance)
                .map(o -> (QueryOp)o).flatMap(o -> o.getQuery().attr().getSet().stream())
                .forEach(triples::add);

        IndexSet<String> vars = new FullIndexSet<>(32);
        streamPreOrder(root).flatMap(o -> o.getAllVars().stream()).forEach(vars::add);
        streamPreOrder(root).forEach(o -> {
            if (o.getOfferedTriplesUniverse() != null) {
                assertNotNull(o.getOfferedVarsUniverse());
            } else {
                o.offerTriplesUniverse(triples);
                o.offerVarsUniverse(vars);
            }
        });

        assertEquals(streamPreOrder(root).map(Op::getOfferedVarsUniverse).distinct().count(), 1);
        assertEquals(streamPreOrder(root).map(Op::getOfferedTriplesUniverse).distinct().count(), 1);
    }

    public static void addUniverseSets(@Nonnull Object queryObj) {
        if (queryObj instanceof Op)
            addUniverseSets((Op)queryObj);
        else
            addUniverseSets((CQuery)queryObj);
    }

    public static @Nonnull List<Op> withUniverseSets(@Nonnull Collection<Op> list) {
        IndexSet<Triple> triples = FullIndexSet.from(list.stream()
                .flatMap(o -> o.getMatchedTriples().stream()).collect(toList()));
        IndexSet<String> vars = FullIndexSet.from(list.stream()
                .flatMap(o -> streamPreOrder(o).flatMap(o2 -> o2.getAllVars().stream()))
                .collect(toList()));
        ArrayList<Op> result = new ArrayList<>(list.size());
        for (Op op : list) {
            Op copy = TreeUtils.deepCopy(op);
            copy.offerTriplesUniverse(triples);
            copy.offerVarsUniverse(vars);
            result.add(copy);
        }
        return result;
    }

    @Test(groups = {"fast"})
    public static class PlanBenchmarksTest extends ConjunctivePlanBenchmarksTestBase {
        @Override protected @Nonnull Class<? extends ConjunctivePlanner> getPlannerClass() {
            return BitsetConjunctivePlanner.class;
        }
    }

}