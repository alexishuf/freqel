package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.model.term.std.StdBlank;
import br.ufsc.lapesd.freqel.model.term.std.StdPlain;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import org.testng.Assert;

import javax.annotation.Nonnull;
import java.util.*;

public class ResultsAssert {
    public static StdBlank universalBlank = new StdBlank("ResultsAssertUniversal");
    private static StdPlain nb = new StdPlain("not-blank");

    public static void assertExpectedResults(@Nonnull Results actual,
                                             @Nonnull Results expected) {
        assertExpectedResults(actual, CollectionResults.greedy(expected).getCollection());
    }
    public static void assertExpectedResults(@Nonnull List<Solution> actual,
                                             @Nonnull Results expected) {
        assertExpectedResults(actual, CollectionResults.greedy(expected).getCollection());
    }
    public static void assertExpectedResults(@Nonnull Results actual,
                                             @Nonnull Collection<? extends Solution> expected) {
        List<Solution> list = new ArrayList<>();
        actual.forEachRemainingThenClose(list::add);
        assertExpectedResults(list, expected);
    }
    public static void assertExpectedResults(@Nonnull List<Solution> actual,
                                             @Nonnull Collection<? extends Solution> expected) {
        actual = cleanBlanks(actual, expected);
        Set<Solution> set = new HashSet<>(actual);
        //noinspection unchecked
        Set<? extends Solution> expectedSet = expected instanceof Set
                ? (Set<? extends Solution>)expected : new LinkedHashSet<>(expected);
        check(expectedSet, set, "Missing solutions");
        check(set, expectedSet, "Unexpected solutions");
        if (expected instanceof List)
            Assert.assertEquals(actual.size(), expected.size());
    }

    public static void assertContainsResults(@Nonnull Results actual,
                                             @Nonnull Collection<? extends Solution> expected) {
        List<Solution> list = new ArrayList<>();
        actual.forEachRemainingThenClose(list::add);
        assertContainsResults(list, expected);
    }

    public static void assertContainsResults(@Nonnull List<Solution> actual,
                                             @Nonnull Collection<? extends Solution> expected) {
        actual = cleanBlanks(actual, expected);
        Set<Solution> set = new HashSet<>(actual);
        //noinspection unchecked
        Set<? extends Solution> expectedSet = expected instanceof Set
                ? (Set<? extends Solution>)expected : new LinkedHashSet<>(expected);
        check(expectedSet, set, "Missing solutions");
    }

    public static
    void assertNotContainsResults(@Nonnull Results actual,
                                  @Nonnull Collection<? extends Solution> notExpected) {
        List<Solution> list = new ArrayList<>();
        actual.forEachRemainingThenClose(list::add);
        assertNotContainsResults(list, notExpected);
    }
    public static
    void assertNotContainsResults(@Nonnull List<Solution> actual,
                                  @Nonnull Collection<? extends Solution> forbidden) {
        HashSet<Solution> set = new HashSet<>(cleanBlanks(actual, forbidden));
        StringBuilder b = new StringBuilder("Forbidden solutions found:");
        boolean shortSolutions = areShortSolutions(forbidden), fail = false;
        for (Solution solution : forbidden) {
            if (set.contains(solution)) {
                fail = true;
                if (shortSolutions) {
                    b.append("\n  ").append(solution.toString());
                } else {
                    solution.forEach((n, t) -> b.append("\n  ").append(n).append('=').append(t));
                    b.append("\n--------------------");
                }
            }
        }
        if (fail)
            Assert.fail(b.toString());
    }

    public static @Nonnull List<Solution>
    cleanBlanks(@Nonnull List<Solution> list, @Nonnull Collection<? extends Solution> expected) {
        boolean hasUniversal = expected.stream().anyMatch(s -> s.getVarNames().stream()
                                       .map(s::get).anyMatch(t -> t != null && t.isBlank()));
        if (!hasUniversal)
            return list;
        ArrayList<Solution> copy = new ArrayList<>(list.size());
        for (Solution solution : list) {
            if (solution.getVarNames().stream().anyMatch(n -> solution.get(n, nb).isBlank())) {
                MapSolution.Builder builder = MapSolution.builder();
                solution.forEach((n, t) -> builder.put(n, t.isBlank() ? universalBlank : t));
                copy.add(builder.build());
            } else {
                copy.add(solution);
            }
        }
        return copy;
    }

    public static @Nonnull String dumpResults(@Nonnull Results results) {
        return dumpResults(results, 0);
    }
    public static @Nonnull String dumpResults(@Nonnull Results results, int indent) {
        return dumpResults(CollectionResults.greedy(results).getCollection(), indent);
    }

    public static @Nonnull String dumpResults(@Nonnull Collection<? extends Solution> collection) {
        return dumpResults(collection, 0);
    }
    public static @Nonnull String dumpResults(@Nonnull Collection<? extends Solution> collection,
                                              int indent) {
        int sum = 0, count = 0;
        for (Solution solution : collection) {
            sum += solution.toString().length();
            ++count;
        }
        StringBuilder indentStrBuilder = new StringBuilder("\n");
        for (int i = 0; i < indent; i++) indentStrBuilder.append(' ');
        String indentStr = indentStrBuilder.toString();

        StringBuilder b = new StringBuilder(sum+count*(1+indent)+count*(indent+20));
        if (sum/(double)count < 80) {
            for (Solution solution : collection)
                b.append(indentStr).append(solution);
        } else {
            StringBuilder innerIndentStrBuilder = new StringBuilder("\n  ");
            for (int i = 0; i < indent; i++)
                innerIndentStrBuilder.append(' ');
            String innerIndentStr = innerIndentStrBuilder.toString();
            for (Solution solution : collection) {
                solution.forEach((n, t)
                        -> b.append(innerIndentStr).append('?').append(n).append('=').append(t));
                b.append(indentStr).append("~~~~~~~~~~~~~~~~~~~~");
            }
        }
        return b.toString();
    }

    private static void check(@Nonnull Set<? extends Solution> superSet,
                              @Nonnull Set<? extends Solution> subSet,
                              @Nonnull String message) {
        //noinspection SuspiciousMethodCalls
        if (subSet.containsAll(superSet))
            return;
        StringBuilder b = new StringBuilder().append(message);
        boolean shortSolutions = areShortSolutions(superSet, subSet);
        for (Solution solution : superSet) {
            if (!subSet.contains(solution)) {
                if (shortSolutions) {
                    b.append("\n  ").append(solution);
                } else {
                    solution.forEach((n, t) -> b.append("\n  ").append(n).append('=').append(t));
                    b.append("\n--------------------");
                }
            }
        }
        Assert.fail(b.toString());
    }

    private static boolean areShortSolutions(@Nonnull Collection<? extends Solution> set) {
        int sum = 0, count = 0;
        for (Solution s : set) {
            sum += s.toString().length();
            ++count;
        }
        return sum/(double)count <= 100;
    }

    private static boolean areShortSolutions(@Nonnull Collection<? extends Solution> expected,
                                             @Nonnull Collection<? extends Solution> actual) {
        int sum = 0, count = 0;
        for (Solution solution : expected) {
            if (!actual.contains(solution)) {
                sum += solution.toString().length();
                ++count;
            }
        }
        return sum/(double)count <= 100;
    }
}
