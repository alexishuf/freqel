package br.ufsc.lapesd.freqel.federation.cardinality.impl;

import br.ufsc.lapesd.freqel.BSBMSelfTest;
import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static br.ufsc.lapesd.freqel.federation.cardinality.impl.QuickSelectivityHeuristic.cost;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.*;

public class QuickSelectivityHeuristicTest implements TestContext {
    private final QuickSelectivityHeuristic heuristic = new QuickSelectivityHeuristic();

    @DataProvider
    public @Nonnull Object[][] instanceData() {
        List<Supplier<CardinalityHeuristic>> suppliers = new ArrayList<>();
        suppliers.add(new Supplier<CardinalityHeuristic>() {
            @Override public CardinalityHeuristic get() { return heuristic; }
            @Override public String toString() { return "shared QuickSelectivityHeuristic"; }
        }); // test state reuse
        suppliers.add(new Supplier<CardinalityHeuristic>() {
            @Override public CardinalityHeuristic get() { return new QuickSelectivityHeuristic(); }
            @Override public String toString() { return "new QuickSelectivityHeuristic"; }
        });
        return suppliers.stream().map(s -> new Object[] {s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "instanceData")
    public void testSinglePatterns(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic heuristic = supplier.get();
        Cardinality o = heuristic.estimate(createQuery(Alice, knows, x));
        Cardinality s = heuristic.estimate(createQuery(x, name, lit("Alice")));
        Cardinality t = heuristic.estimate(createQuery(x, type, Person));
        Cardinality p = heuristic.estimate(createQuery(Alice, x, Bob));
        Cardinality po = heuristic.estimate(createQuery(Alice, x, y));
        Cardinality sp = heuristic.estimate(createQuery(x, y, Bob));

        for (Cardinality cardinality : asList(t, p, o, po, sp))
            assertEquals(cardinality.getReliability(), Cardinality.Reliability.GUESS);

        assertTrue(t.getValue(-1) > o.getValue(-1));
        assertTrue(t.getValue(-1) > p.getValue(-1));
        assertTrue(t.getValue(-1) > s.getValue(-1));
        assertTrue(s.getValue(-1) > o.getValue(-1));
        assertTrue(s.getValue(-1) > p.getValue(-1));
        assertTrue(p.getValue(-1) < o.getValue(-1));

        assertTrue(po.getValue(-1) > o.getValue(-1));
        assertTrue(po.getValue(-1) > p.getValue(-1));
        assertTrue(po.getValue(-1) < s.getValue(-1));
        assertTrue(po.getValue(-1) < t.getValue(-1));

        assertTrue(sp.getValue(-1) > po.getValue(-1));
    }

    @DataProvider
    public @Nonnull Object[][] benchmarkData() throws Exception {
        List<ImmutablePair<String, CQuery>> queries = new ArrayList<>();
        for (String filename : BSBMSelfTest.QUERY_FILENAMES) {
            TreeUtils.streamPreOrder(BSBMSelfTest.loadQuery(filename))
                    .filter(QueryOp.class::isInstance)
                    .map(o -> ((QueryOp)o).getQuery())
                    .forEach(q -> queries.add(ImmutablePair.of("BSBM."+filename, q)));
        }
        for (String filename : LargeRDFBenchSelfTest.QUERY_FILENAMES) {
            TreeUtils.streamPreOrder(LargeRDFBenchSelfTest.loadQuery(filename))
                    .filter(QueryOp.class::isInstance)
                    .map(o -> ((QueryOp)o).getQuery())
                    .forEach(q -> queries.add(ImmutablePair.of("LRB."+filename, q)));
        }

        return Arrays.stream(instanceData())
                .flatMap(arr -> queries.stream().map(pair -> {
                    Object[] copy = Arrays.copyOf(arr, 3);
                    copy[1] = pair.left;
                    copy[2] = pair.right;
                    return copy;
                })).toArray(Object[][]::new);
    }

    @Test(dataProvider = "benchmarkData")
    public void testBenchmarks(@Nonnull Supplier<CardinalityHeuristic> supplier,
                               @Nonnull String queryName,
                               @Nonnull CQuery query) {
        // test it doesn't fail horribly (exceptions, infinite recursion, etc.)
        Cardinality estimate = supplier.get().estimate(query);
        assertEquals(estimate.getReliability(), Cardinality.Reliability.GUESS);
        assertNotEquals(estimate.getValue(-1), -1);
    }

    @Test(dataProvider = "benchmarkData")
    public void testBenchmarksParallel(@Nonnull Supplier<CardinalityHeuristic> supplier,
                                       @Nonnull String queryName,
                                       @Nonnull CQuery query) throws Exception {
        // test it doesn't fail horribly (exceptions, infinite recursion, etc.)
        CardinalityHeuristic h = supplier.get();
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 20; i++) {
            futures.add(executor.submit(() -> {
                Cardinality estimate = h.estimate(query);
                assertEquals(estimate.getReliability(), Cardinality.Reliability.GUESS);
                assertNotEquals(estimate.getValue(-1), -1);
                return null;
            }));
        }
        for (Future<?> future : futures)
            future.get(); // rethrows AssertionErrors
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    }

    @Test(dataProvider = "instanceData")
    public void testPathBonus(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic heuristic = supplier.get();
        long single = heuristic.estimate(createQuery(x, knows, y)).getValue(-1);
        Cardinality path = heuristic.estimate(createQuery(x, knows, y, y, knows, z));
        assertTrue(path.getValue(-1) < single*2);
    }

    @Test(dataProvider = "instanceData")
    public void testGroundSubjectBonus(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        long bonus  = h.estimate(createQuery(Alice, knows, x, x, knows, y)).getValue(-1);
        long normal = h.estimate(createQuery(x,     knows, y, y, knows, z)).getValue(-1);
        assertTrue(bonus < normal);
    }

    @Test(dataProvider = "instanceData")
    public void testStarBonus(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        long s1 = h.estimate(createQuery(x, p1, o1)).getValue(-1);
        long s2 = h.estimate(createQuery(x, p1, o1, x, p2, o2)).getValue(-1);
        long s3 = h.estimate(createQuery(x, p1, o1, x, p2, o2, x, p3, o2)).getValue(-1);
        assertTrue(s1 > s2);
        assertTrue(s2 > s3);

        long path1 = h.estimate(createQuery(Alice, p1, x, x, p2, o2)).getValue(-1);
        assertTrue(s2 > path1);

        long path2 = h.estimate(createQuery(Alice, p1, o1, Alice, p2, o2)).getValue(-1);
        assertTrue(s2 > path2);
    }

    @Test(dataProvider = "instanceData")
    public void testTypedStarBonus(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        long max = h.estimate(createQuery(x, p1, o1)).getValue(-1);
        Cardinality est = h.estimate(createQuery(x, p1, o1, x, p2, o2, x, type, Person));
        assertEquals(est.getReliability(), Cardinality.Reliability.GUESS);
        assertTrue(est.getValue(-1) < max);
    }

    @Test(dataProvider = "instanceData")
    public void testApplyGroundObjectRateOnSingleStar(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        Cardinality ground = h.estimate(createQuery(x, p1, o1, x, p2, o2, x, p3, Bob));
        Cardinality free   = h.estimate(createQuery(x, p1, o1, x, p2, o2, x, p3, o3));
        assertTrue(ground.getValue(-1) < free.getValue(-1));
    }

    @Test(dataProvider = "instanceData")
    public void testGroundStarBonus(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        Cardinality free   = h.estimate(createQuery(x, p1, o1, x, p2, o2));
        Cardinality ground = h.estimate(createQuery(x, p1, o1, x, p2, o2, x, p3, Alice));
        assertTrue(ground.getValue(-1) < free.getValue(-1));
    }

    @Test(dataProvider = "instanceData")
    public void testS10Similar(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        long bigger = h.estimate(createQuery(x, p1, y, y, type, Person)).getValue(-1);
        long smaller = h.estimate(createQuery(x, p1, o1, x, p2, o2, x, p3, o3)).getValue(-1);
        assertTrue(smaller < bigger);
    }

    @Test(dataProvider = "instanceData")
    public void testS10(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        long bigger  = h.estimate(createQuery(y, sameAs, z, z, type, Person)).getValue(-1);
        long biggerR = h.estimate(createQuery(z, type, Person, y, sameAs, z)).getValue(-1);
        long bigger0 = h.estimate(createQuery(y, sameAs, z)).getValue(-1);
        long bigger1 = h.estimate(createQuery(y, type, Person)).getValue(-1);
        long smaller = h.estimate(createQuery(x, p1, y, x, p2, o2, x, p3, o3)).getValue(-1);
        assertEquals(bigger, biggerR);
        assertTrue(smaller < bigger0);
        assertTrue(smaller < bigger1);
        assertTrue(smaller < bigger);


        long smaller0 = h.estimate(createQuery(
                x, p1, y, x, p2, o2, x, p3, o3,
                y, sameAs, z
        )).getValue(-1);

        assertTrue(smaller0 < bigger);
        assertTrue(smaller0 > smaller); // sameAs penalty

        long prod = h.estimate(createQuery(
                x, p1, y, x, p2, o2, x, p3, o3,
                z, type, Person
        )).getValue(-1);

        assertTrue(prod > smaller0);
        assertTrue(prod > bigger);
        assertTrue(prod > bigger1);
    }

    @Test(dataProvider = "instanceData")
    public void testS12(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        MutableCQuery star2 = createQuery(x, p1, o1, x, p2, o2);
        MutableCQuery star3 = createQuery(x, type, Person, x, p1, o1, x, p2, y);
        MutableCQuery join = createQuery(x, type, Person, x, p1, o1, x, p2, y,
                                         y, p3, o3);
        long star2Est = h.estimate(star2).getValue(-1);
        long star3Est = h.estimate(star3).getValue(-1);
        long joinEst = h.estimate(join).getValue(-1);

        assertTrue(star3Est < star2Est);
        assertTrue(star3Est < joinEst);
        assertTrue(joinEst < star2Est);
    }

    @Test(dataProvider = "instanceData")
    public void testObjObjJoin(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();

        long prod  = h.estimate(createQuery(x, p1, o1, y,     p2, o2)).getValue(-1);
        long oo    = h.estimate(createQuery(x, p1, o1, y,     p2, o1)).getValue(-1);
        long ooGnd = h.estimate(createQuery(x, p1, o1, Alice, p2, o1)).getValue(-1);

        assertTrue(oo < prod);
        assertTrue(oo < 0.8*prod);
        assertTrue(ooGnd < oo);
    }

    @Test(dataProvider = "instanceData")
    public void testGroundObjectBonus(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic h = supplier.get();
        long bonus  = h.estimate(createQuery(x, knows, y, y, knows, Bob)).getValue(-1);
        long normal = h.estimate(createQuery(x, knows, y, y, knows, z  )).getValue(-1);
        assertTrue(bonus < normal);
    }

    @Test(dataProvider = "instanceData")
    public void testDoubleEndedPath(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic heuristic = supplier.get();
        Cardinality card = heuristic.estimate(createQuery(x, knows, y, y, knows, z));
        int min = Math.min(QuickSelectivityHeuristic.cost(x, knows, Bob), QuickSelectivityHeuristic.cost(Alice, knows, y)) + 2;
        assertTrue(card.getValue(-1) > min);
    }

    @Test(dataProvider = "instanceData")
    public void testTrivialCycle(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic heuristic = supplier.get();
        MutableCQuery simple = createQuery(x, knows, x);
        assertEquals(heuristic.estimate(simple).getValue(-1),
                     QuickSelectivityHeuristic.cost(x, knows, y));
        assertEquals(heuristic.estimate(simple), heuristic.estimate(createQuery(x, knows, y)));
    }

    @Test(dataProvider = "instanceData")
    public void testIndirectCycle(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic heuristic = supplier.get();
        MutableCQuery q = createQuery(x, knows, y, y, knows, z, z, knows, x);
        Cardinality cardinality = heuristic.estimate(q); //does not fail
        long value = cardinality.getValue(-1);
        assertTrue(value > 0); //valid value
        assertTrue(value > heuristic.estimate(createQuery(Alice, knows, y)).getValue(-1));
        assertTrue(value > heuristic.estimate(createQuery(x, knows, Alice)).getValue(-1));
        long single = heuristic.estimate(createQuery(x, knows, y)).getValue(-1);
        assertTrue(value >= 0.8*single);
    }

    @Test(dataProvider = "instanceData")
    public void testBadCartesian(@Nonnull Supplier<CardinalityHeuristic> supplier) {
        CardinalityHeuristic heuristic = supplier.get();
        MutableCQuery bad = createQuery(Alice, name, x, Bob, name, y);

        Cardinality est0 = heuristic.estimate(CQuery.from(bad.get(0)));
        Cardinality est1 = heuristic.estimate(CQuery.from(bad.get(1)));
        assertTrue(est0.getValue(-1) >= cost(bad.get(0)));
        assertTrue(est1.getValue(-1) >= cost(bad.get(1)));

        Cardinality est = heuristic.estimate(bad);
        assertEquals(est.getReliability(), Cardinality.Reliability.GUESS);
        long min = est0.getValue(-1) + est1.getValue(-1);
        assertTrue(min >= 2L* QuickSelectivityHeuristic.cost(Alice, name, x));
        assertTrue(est.getValue(-1) > min);
    }


}