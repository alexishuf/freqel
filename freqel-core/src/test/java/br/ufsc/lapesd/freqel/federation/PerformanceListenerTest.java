package br.ufsc.lapesd.freqel.federation;

import br.ufsc.lapesd.freqel.NamedSupplier;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.ThreadedPerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.performance.metrics.impl.SimpleMetric;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class PerformanceListenerTest {
    public static final @Nonnull List<Class<? extends PerformanceListener>> classes = asList(
            NoOpPerformanceListener.class, ThreadedPerformanceListener.class
    );
    public static final @Nonnull List<Class<? extends PerformanceListener>> storingClasses
            = singletonList(ThreadedPerformanceListener.class);


    private static final SimpleMetric<Integer> singleMetric =
            SimpleMetric.builder("singleTest").singleValued().create(Integer.class);

    private PerformanceListener listener = null;

    @AfterMethod
    public void tearDown() {
        if (listener != null)
            listener.close();
    }

    @DataProvider
    public static Object[][] allSuppliersData() {
        return classes.stream().map(s -> new Object[]{new NamedSupplier<>(s)})
                      .toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] storingSuppliersData() {
        return storingClasses.stream().map(s -> new Object[]{new NamedSupplier<>(s)})
                             .toArray(Object[][]::new);
    }

    @Test(dataProvider = "allSuppliersData")
    public void testSingleValueFromEmpty(Supplier<PerformanceListener> supplier) {
        listener = supplier.get();
        assertNull(listener.getValue(singleMetric));
        assertEquals(listener.getValue(singleMetric, 1), Integer.valueOf(1));
        assertEquals(listener.getValues(singleMetric).size(), 0);
    }

    @Test(dataProvider = "allSuppliersData")
    public void testValueFromEmpty(Supplier<PerformanceListener> supplier) {
        listener = supplier.get();
        assertNull(listener.getValue(Metrics.PLAN_MS));
        assertEquals(listener.getValue(Metrics.PLAN_MS, -2.0), -2.0);
        assertEquals(listener.getValues(Metrics.PLAN_MS).size(), 0);
    }

    @Test(dataProvider = "storingSuppliersData")
    public void testStoreSingleValue(Supplier<PerformanceListener> supplier) {
        listener = supplier.get();
        listener.sample(singleMetric, 1);
        listener.sync();
        assertEquals(listener.getValue(singleMetric, 0), Integer.valueOf(1));

        listener.sample(singleMetric, 2);
        listener.sync();
        assertEquals(listener.getValue(singleMetric, 0), Integer.valueOf(2));
        assertEquals(listener.getValues(singleMetric), singletonList(2));
    }

    @Test(dataProvider = "storingSuppliersData")
    public void testStoreValues(Supplier<PerformanceListener> supplier) {
        listener = supplier.get();
        listener.sample(Metrics.PLAN_MS, 23.43);
        listener.sample(Metrics.PLAN_MS, 24.43);
        listener.sample(Metrics.PLAN_MS, 25.43);
        listener.sync();

        assertEquals(listener.getValues(Metrics.PLAN_MS), asList(23.43, 24.43, 25.43));

        listener.sample(singleMetric, 1);
        listener.sync();
        assertEquals(listener.getValues(Metrics.PLAN_MS), asList(23.43, 24.43, 25.43));
        assertEquals(listener.getValues(singleMetric), singletonList(1));

        listener.sample(Metrics.OPT_MS, 27.2);
        listener.sample(Metrics.OPT_MS, 28.2);
        listener.sync();
        assertEquals(listener.getValues(Metrics.PLAN_MS), asList(23.43, 24.43, 25.43));
        assertEquals(listener.getValues(Metrics.OPT_MS), asList(27.2, 28.2));
        assertEquals(listener.getValues(singleMetric), singletonList(1));
    }

    @Test(dataProvider = "storingSuppliersData")
    public void testMultiThreadStoreValues(Supplier<PerformanceListener> supplier) throws InterruptedException {
        listener = supplier.get();
        HashSet<Integer> expected = new HashSet<>(10000);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10000; j++) expected.add(i * j);
        }

        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            for (int run = 0; run < 10; run++) {
                CountDownLatch latch = new CountDownLatch(10);
                for (int i = 0; i < 10; i++) {
                    int taskIndex = i;
                    executor.execute(() -> {
                        for (int j = 0; j < 50000; j++) {
                            listener.sample(singleMetric, taskIndex * j);
                            if (taskIndex % 3 == 0 && j < 25000)
                                listener.sync(); //just to introduce some chaos
                        }
                        for (int j = 0; j < 10000; j++)
                            listener.sample(Metrics.SOURCES_COUNT, taskIndex * j);
                        latch.countDown();
                    });
                }
                latch.await(); // all sample() calls have been issued
                listener.sync(); //wait until their effect is visible
                HashSet<Integer> actual = new HashSet<>(listener.getValues(Metrics.SOURCES_COUNT));
                assertEquals(actual, expected);

            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));
        }

    }
}