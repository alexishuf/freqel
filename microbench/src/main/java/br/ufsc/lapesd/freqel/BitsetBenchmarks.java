package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.bitset.ArrayBitset;
import br.ufsc.lapesd.freqel.util.bitset.DynamicBitset;
import br.ufsc.lapesd.freqel.util.bitset.LongBitset;
import br.ufsc.lapesd.freqel.util.bitset.SegmentBitset;
import com.google.common.base.Stopwatch;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@State(Scope.Thread)
public class BitsetBenchmarks {
    //       1   16   32   64    128
    @Param({"1", "4", "8", "16", "32"})
    private int bitsSet;

    @Param({"segment", "long", "array", "dynamic"})
    private String implementation;

    private Supplier<Bitset> factory;
    private Bitset rhs;

    private @Nonnull Bitset createAndFill(int from, int step, int count) {
        Bitset bs = factory.get();
        for (int i = from, end = from + step*count; i < end; i++)
            bs.set(i);
        return bs;
    }

    @Setup(Level.Trial) public void setUp() {
        int bitsSize = (bitsSet - 1) * 4 + 1;
        if (implementation.equals("segment")) {
            factory = () -> {
                long[] store = new long[2 + (bitsSize >> 6) + 1 + 1];
                return new SegmentBitset(store, 2, store.length-1);
            };
        } else if (implementation.equals("long")) {
            factory = bitsSize > 64 ? () -> new ArrayBitset(bitsSize) : LongBitset::new;
        } else if (implementation.equals("array")) {
            factory = () -> new ArrayBitset(bitsSize);
        } else if (implementation.equals("dynamic")) {
            factory = DynamicBitset::new;
        }
        rhs = createAndFill(bitsSet*4/2, 4, bitsSet/2);
    }

    public static void main(String[] args) throws IOException {
        BitsetBenchmarks b = new BitsetBenchmarks();
        b.implementation = "dynamic";
        b.bitsSet = 16;
        b.setUp();
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(TimeUnit.SECONDS) < 2)
            b.andNotBenchmark();
        System.out.println("Hit ENTER to run...");
        //noinspection ResultOfMethodCallIgnored
        System.in.read();
        stopwatch.reset().start();
        while (stopwatch.elapsed(TimeUnit.SECONDS) < 30)
            b.andNotBenchmark();
    }

    @Benchmark public int andNotBenchmark() {
        Bitset lhs = createAndFill(0, 4, bitsSet);
        lhs.andNot(rhs);
        return lhs.hashCode();
    }

    @Benchmark public int createIntersectionBenchmark() {
        Bitset lhs = createAndFill(0, 4, bitsSet);
        return lhs.createAnd(rhs).hashCode();
    }

    @Benchmark public int and() {
        Bitset lhs = createAndFill(0, 4, bitsSet);
        lhs.and(rhs);
        return lhs.hashCode();
    }
}
