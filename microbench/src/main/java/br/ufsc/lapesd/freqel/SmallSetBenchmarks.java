package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.util.ArraySet;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import com.google.common.collect.ImmutableSet;
import org.openjdk.jmh.annotations.*;

import java.util.*;

@SuppressWarnings("UseBulkOperation")
@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class SmallSetBenchmarks {

    @Param({"0", "1", "4", "16"})
    int size;

    private List<String> strings;
    private Set<String> stringsArraySet, stringsIndexedSet, stringsHashSet, stringsImmutableSet;

    @Setup(Level.Trial)
    public void setUp() {
        strings  = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            strings .add("Example1-"+i);
        }
        Random rnd = new Random();
        rnd.setSeed(37); //same ordering for all benchmarks
        Collections.shuffle(strings, rnd);

        stringsArraySet = ArraySet.fromDistinct(strings);
        stringsIndexedSet = FullIndexSet.fromDistinct(strings);
        stringsHashSet = new HashSet<>(strings);
        stringsImmutableSet = ImmutableSet.copyOf(strings);
    }

    @Benchmark
    public Set<String> arraySetCreation() {
        return ArraySet.fromDistinct(strings);
    }

    @Benchmark
    public Set<String> indexedSetCreation() {
        return FullIndexSet.fromDistinctCopy(strings);
    }

    @Benchmark
    public Set<String> indexedSetStealCreation() {
        return FullIndexSet.fromDistinct(strings);
    }

    @Benchmark
    public Set<String> hashSetCreation() {
        return new HashSet<>(strings);
    }

    @Benchmark
    public Set<String> immutableSetCreation() {
        return ImmutableSet.copyOf(strings);
    }

    @Benchmark
    public List<String> arraySetIteration() {
        List<String> sink = new ArrayList<>(size);
        for (String s : ArraySet.fromDistinct(strings))
            sink.add(s);
        return sink;
    }

    @Benchmark
    public List<String> indexedSetIteration() {
        List<String> sink = new ArrayList<>(size);
        for (String s : FullIndexSet.fromDistinctCopy(strings))
            sink.add(s);
        return sink;
    }

    @Benchmark
    public List<String> hashSetIteration() {
        List<String> sink = new ArrayList<>(size);
        for (String s : new HashSet<>(strings))
            sink.add(s);
        return sink;
    }

    @Benchmark
    public List<String> immutableSetIteration() {
        List<String> sink = new ArrayList<>(size);
        for (String s : ImmutableSet.copyOf(strings))
            sink.add(s);
        return sink;
    }

    @Benchmark
    public boolean arraySetSelfEquals() {
        return ArraySet.fromDistinct(strings).equals(stringsArraySet);
    }

    @Benchmark
    public boolean indexedSetSelfEquals() {
        return FullIndexSet.fromDistinct(strings).equals(stringsIndexedSet);
    }

    @Benchmark
    public boolean hashSetSelfEquals() {
        return new HashSet<>(strings).equals(stringsHashSet);
    }

    @Benchmark
    public boolean immutableSetSelfEquals() {
        return ImmutableSet.copyOf(strings).equals(stringsImmutableSet);
    }
}
