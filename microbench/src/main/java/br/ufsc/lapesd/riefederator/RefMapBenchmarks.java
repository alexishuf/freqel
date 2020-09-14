package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.deprecated.*;
import br.ufsc.lapesd.riefederator.util.RefHashMap;
import br.ufsc.lapesd.riefederator.util.RefMap;
import com.google.common.collect.Maps;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class RefMapBenchmarks {

    @Param({"0", "4", "8", "16", "32", "128", "1024"})
    private int size;

    @Param({"hash", "hash+shift", "hash+simple", "hash+fastgrowth", "sorted", "sorted+Pair", "RefEquals+HashSet"})
    private String implementation;

    public static class Thing {
        private int id;

        public Thing(int id) {
            this.id = id;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Thing)) return false;
            Thing thing = (Thing) o;
            return id == thing.id;
        }

        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }

    private Map<Thing, Integer> filledMap;
    private List<Thing> filledKeys;
    private Function<Integer, Map<Thing, Integer>> capacityFactory;
    private Supplier<Map<Thing, Integer>> factory;
    private Function<Object, Object> putAllFunction;

    @Setup(Level.Trial)
    public void setUp() {
        if (implementation.equals("hash")) {
            capacityFactory = c -> new RefHashMap<Thing, Integer>(c);
            factory = () -> new RefHashMap<>();
            putAllFunction = this::putAllRefMap;
        } else if (implementation.equals("hash+shift")) {
            capacityFactory = c -> new RefHashMapShift<>(c);
            factory = () -> new RefHashMapShift<>();
            putAllFunction = this::putAllRefMap;
        } else if (implementation.equals("hash+simple")) {
            capacityFactory = c -> new RefHashMapSimpleGrowth<>(c);
            factory = () -> new RefHashMapSimpleGrowth<>();
            putAllFunction = this::putAllRefMap;
        } else if (implementation.equals("hash+fastgrowth")) {
            capacityFactory = c -> new RefHashMapFastGrowth<>(c);
            factory = () -> new RefHashMapFastGrowth<>();
            putAllFunction = this::putAllRefMap;
        } else if (implementation.equals("sorted")) {
            capacityFactory = c -> new RefSortedMap<>(c);
            factory = () -> new RefSortedMap<>();
            putAllFunction = this::putAllRefMap;
        } else if (implementation.equals("sorted+Pair")) {
            capacityFactory = c -> new RefSortedPairMap<>(c);
            factory = () -> new RefSortedPairMap<>();
            putAllFunction = this::putAllRefMap;
        } else if (implementation.equals("RefEquals+HashSet")) {
            capacityFactory = c -> Maps.newHashMapWithExpectedSize(c);
            factory = () -> new HashMap<>();
            putAllFunction = this::putAllRefEquals;
        } else {
            throw new RuntimeException("Bad implementation="+implementation);
        }
        filledMap = capacityFactory.apply(size);
        filledKeys = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Thing key = new Thing(i);
            filledKeys.add(key);
            filledMap.put(key, i);
        }
    }

    private @Nonnull RefMap<Thing, Integer> putAllRefMap(@Nonnull Object in) {
        @SuppressWarnings("unchecked") RefMap<Thing, Integer> map = (RefMap<Thing, Integer>) in;
        for (int i = 0; i < size; i++)
            map.put(new Thing(i), i);
        return map;
    }

    private @Nonnull Map<RefEquals<Thing>, Integer> putAllRefEquals(@Nonnull Object in) {
        @SuppressWarnings("unchecked")
        Map<RefEquals<Thing>, Integer> map = (Map<RefEquals<Thing>, Integer>) in;
        for (int i = 0; i < size; i++)
            map.put(RefEquals.of(new Thing(i)), i);
        return map;
    }

    @Benchmark public @Nonnull Object putAllBenchmark() {
        return putAllFunction.apply(factory.get());
    }

    @Benchmark public @Nonnull Object putAllReservedBenchmark() {
        return putAllFunction.apply(capacityFactory.apply(size));
    }

    @Benchmark public int iterateBenchmark() {
        int sum = 0;
        for (Map.Entry<Thing, Integer> e : filledMap.entrySet())
            sum += e.getKey().id + e.getValue();
        return sum;
    }

    @Benchmark public int getAllBenchmark() {
        int sum = 0;
        for (int i = 0; i < size; i++)
            sum += filledMap.get(filledKeys.get(i));
        return sum;
    }

    @Benchmark public int getEvenBenchmark() {
        int sum = 0;
        for (int i = 0; i < size; i += 2)
            sum += filledMap.get(filledKeys.get(i));
        return sum;
    }
}
