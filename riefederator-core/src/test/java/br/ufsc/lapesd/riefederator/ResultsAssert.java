package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import org.testng.Assert;

import javax.annotation.Nonnull;
import java.util.*;

public class ResultsAssert {
    public static void assertExpectedResults(@Nonnull Results actual,
                                             @Nonnull Results expected) {
        assertExpectedResults(actual, CollectionResults.greedy(expected).getCollection());
    }

    public static void assertExpectedResults(@Nonnull Results actual,
                                             @Nonnull Collection<? extends Solution> expected) {
        List<Solution> list = new ArrayList<>();
        actual.forEachRemainingThenClose(list::add);
        Set<Solution> set = new HashSet<>(list);
        //noinspection unchecked
        Set<? extends Solution> expectedSet = expected instanceof Set
                ? (Set<? extends Solution>)expected : new LinkedHashSet<>(expected);
        check(expectedSet, set, "Missing solutions");
        check(set, expectedSet, "Unexpected solutions");
        if (expected instanceof List)
            Assert.assertEquals(list.size(), expected.size());
    }

    private static void check(@Nonnull Set<? extends Solution> superSet,
                              @Nonnull Set<? extends Solution> subSet,
                              @Nonnull String message) {
        if (subSet.containsAll(superSet)) return;
        StringBuilder b = new StringBuilder().append(message);
        boolean shortSolutions = areShortSolutions(superSet, subSet);
        for (Solution solution : superSet) {
            if (!subSet.contains(solution)) {
                if (shortSolutions) {
                    b.append("\n  ").append(solution);
                } else {
                    solution.forEach((n, t) -> {
                        b.append("\n  ").append(n).append('=').append(t);
                    });
                }
            }
        }
        Assert.fail(b.toString());
    }

    private static boolean areShortSolutions(@Nonnull Set<? extends Solution> expected,
                                             @Nonnull Set<? extends Solution> actual) {
        int sum = 0, count = 0;
        for (Solution solution : expected) {
            if (!actual.contains(solution)) {
                sum += solution.toString().length();
                ++count;
            }
        }
        return sum/(double)count <= 80;
    }
}
