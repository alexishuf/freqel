package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;


public class BitsetConjunctivePlannerTest {

    public static @Nonnull List<Op> withUniverseSets(@Nonnull Collection<Op> list) {
        IndexSet<Triple> triples = FullIndexSet.from(list.stream()
                .flatMap(o -> o.getMatchedTriples().stream()).collect(toList()));
        IndexSet<String> vars = FullIndexSet.from(list.stream()
                .flatMap(o -> TreeUtils.streamPreOrder(o).flatMap(o2 -> o2.getAllVars().stream()))
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