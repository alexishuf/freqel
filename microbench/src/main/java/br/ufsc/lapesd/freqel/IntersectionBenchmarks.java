package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.util.CollectionUtils;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.Collections.emptySet;

@State(Scope.Thread)
public class IntersectionBenchmarks {
    public static final int SEED = 269611621;
    public static final String LEFT_LETTERS = "uvxyz";
    public static final String RIGHT_LETTERS = "abcde";

    private List<ImmutablePair<Set<String>, Set<String>>> inputs;
    private List<ImmutablePair<List<String>, List<String>>> listInputs;
    private List<ImmutablePair<IndexSubset<String>, IndexSubset<String>>> indexedInputs;
    private IndexSet<String> universe;
    private Random random;


    /**
     * Generates a input dataset with average total vars = 5 and average intersection = 0.9
     * These values come from the observed in JoinInfo for the planning of all LRB and BSBM queries
     */
    @Setup
    public void setUp() {
        universe = computeUniverse();
        random = new Random(SEED);
        inputs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Set<String> l = createSet(LEFT_LETTERS, 2 + random.nextInt(2));
            Set<String> r = createSet(RIGHT_LETTERS, 2 + random.nextInt(2));
            inputs.add(ImmutablePair.of(l, r));
        }
        for (int i = 0; i < 3; i++) {
            Set<String> l = createSet(LEFT_LETTERS, 2+ random.nextInt(2), "k1");
            Set<String> r = createSet(RIGHT_LETTERS, 2 + random.nextInt(2), "k1");
            inputs.add(ImmutablePair.of(l, r));
        }
        for (int i = 0; i < 3; i++) {
            Set<String> l = createSet(LEFT_LETTERS, 2+ random.nextInt(2), "k1", "k2");
            Set<String> r = createSet(RIGHT_LETTERS, 2 + random.nextInt(2), "k2", "k1");
            inputs.add(ImmutablePair.of(l, r));
        }
        indexedInputs = new ArrayList<>();
        for (ImmutablePair<Set<String>, Set<String>> pair : inputs)
            indexedInputs.add(index(pair));
        listInputs = new ArrayList<>();
        for (ImmutablePair<Set<String>, Set<String>> p : inputs)
            listInputs.add(ImmutablePair.of(new ArrayList<>(p.left), new ArrayList<>(p.right)));
    }

    private @Nonnull IndexSet<String> computeUniverse() {
        List<String> universeValues = new ArrayList<>();
        addAllPossibleVars(universeValues, LEFT_LETTERS);
        addAllPossibleVars(universeValues, RIGHT_LETTERS);
        addAllPossibleVars(universeValues, "k");
        return FullIndexSet.fromDistinct(universeValues);
    }

    private void addAllPossibleVars(List<String> universeValues, String letters) {
        for (int i = 0; i < letters.length(); i++) {
            for (int j = 0; j < 3; j++)
                universeValues.add(letters.substring(i, i + 1) + j);
        }
    }

    private @Nonnull ImmutablePair<IndexSubset<String>, IndexSubset<String>>
    index(ImmutablePair<Set<String>, Set<String>> pair) {
        IndexSubset<String> l = universe.subset(pair.left);
        IndexSubset<String> r = universe.subset(pair.right);
        if (l.size() != pair.left.size() || r.size() != pair.right.size())
            throw new RuntimeException("universe misses variables");
        return ImmutablePair.of(l, r);
    }

    private @Nonnull Set<String> createSet(@Nonnull String letters, int size, String... include) {
        HashSet<String> set = Sets.newHashSetWithExpectedSize(size);
        if (include.length > size)
            throw new IllegalArgumentException("include.lengt= > size");
        Collections.addAll(set, include);
        for (int i = include.length; i < size; i++) {
            int letterIdx = random.nextInt(letters.length());
            String v = letters.substring(letterIdx, letterIdx+1) + i;
            if (!set.add(v))
                throw new IllegalArgumentException("include clashes with generated vars");
        }
        return set;
    }

    @Benchmark
    public int collectionUtilsList() {
        int total = 0;
        for (ImmutablePair<List<String>, List<String>> pair : listInputs)
            total += CollectionUtils.intersect(pair.left, pair.right).size();
        return total;
    }

    @Benchmark
    public int collectionUtilsSet() {
        int total = 0;
        for (ImmutablePair<Set<String>, Set<String>> pair : inputs)
            total += CollectionUtils.intersect(pair.left, pair.right).size();
        return total;
    }

    @Benchmark
    public int collectionUtilsSetAsCollection() {
        int total = 0;
        for (ImmutablePair<Set<String>, Set<String>> pair : inputs) {
            Collection<String> left = pair.left, right = pair.right;
            total += CollectionUtils.intersect(left, right).size();
        }
        return total;
    }

    public static @Nonnull <T> Set<T> oldIntersect(@Nullable Collection<T> left,
                                                   @Nullable Collection<T> right) {
        if (left == null || right == null)
            return emptySet();
        final int ls = left.size(), rs = right.size();
        if (ls == 0 || rs == 0)
            return emptySet();
        if (rs < ls) {
            Collection<T> tmp = left;
            left = right;
            right = tmp;
        }
        Set<T> result = new HashSet<>(left);
        result.retainAll(right);
        return result;
    }

    @Benchmark
    public int oldIntersect() {
        int total = 0;
        for (ImmutablePair<Set<String>, Set<String>> pair : inputs)
            total += oldIntersect(pair.left, pair.right).size();
        return total;
    }

    @Benchmark
    public int oldIntersectList() {
        int total = 0;
        for (ImmutablePair<List<String>, List<String>> pair : listInputs)
            total += oldIntersect(pair.left, pair.right).size();
        return total;
    }

    private @Nonnull Set<String> oldIntersectSet(@Nonnull Set<String> lv,
                                                 @Nonnull Set<String> rv) {
        int lvs = lv.size(), rvs = rv.size();
        if (lvs == 0 || rvs == 0) {
            return emptySet();
        } else if (rvs < lvs) {
            Set<String> tmp = lv; lv = rv; rv = tmp;
        }
        Set<String> result = new HashSet<>(lv);
        result.retainAll(rv); //joinVars has the intersection of lv & rv
        return result;
    }

    @Benchmark
    public int oldIntersectSet() {
        int total = 0;
        for (ImmutablePair<Set<String>, Set<String>> pair : inputs)
            total += oldIntersectSet(pair.left, pair.right).size();
        return total;
    }

    @Benchmark
    public int indexed() {
        int total = 0;
        for (ImmutablePair<IndexSubset<String>, IndexSubset<String>> pair : indexedInputs) {
            total += pair.left.createIntersection(pair.right).size();
        }
        return total;
    }
}
