package br.ufsc.lapesd.freqel.federation.performance;

import br.ufsc.lapesd.freqel.util.NamedSupplier;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.performance.metrics.TimeSampler;
import com.google.common.collect.Sets;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.*;
import static org.testng.Assert.*;

public class SamplerRegistryTest {
    public static final List<NamedSupplier<SamplerRegistry >> suppliers =
            singletonList(new NamedSupplier<>(SimpleSamplerRegistry.class));
    private ThreadedPerformanceListener listener;

    @BeforeMethod
    public void setUp() {
        listener = new ThreadedPerformanceListener();
    }

    @AfterMethod
    public void tearDown() {
        listener.close();
    }

    @DataProvider
    public static Object[][] suppliersData() {
        return suppliers.stream().map(s -> new Object[] {s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "suppliersData")
    public void testCreateAndSample(Supplier<SamplerRegistry> supplier) throws InterruptedException {
        SamplerRegistry r = supplier.get();
        try (TimeSampler sampler = r.createSampler(Metrics.PLAN_MS, listener)) {
            assertNotNull(sampler);
            Thread.sleep(50);
        }
        listener.sync();
        List<Double> values = listener.getValues(Metrics.PLAN_MS);
        assertEquals(values.size(), 1);
        assertTrue(values.get(0) >= 40);
    }

    @Test(dataProvider = "suppliersData")
    public void testGetSampler(Supplier<SamplerRegistry> supplier) throws InterruptedException {
        SamplerRegistry r = supplier.get();
        try (TimeSampler planSampler = r.createSampler(Metrics.PLAN_MS, listener)) {
            assertNotNull(planSampler);
            assertSame(r.getCurrentThreadSampler(Metrics.PLAN_MS), planSampler);
            assertNull(r.getCurrentThreadSampler(Metrics.OPT_MS));
            boolean[] ok = {false};
            Thread thread = new Thread(() -> {
                assertNull(r.getCurrentThreadSampler(Metrics.PLAN_MS));
                ok[0] = true;
            });
            thread.start();
            thread.join();
            assertTrue(ok[0]);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testListSamplers(Supplier<SamplerRegistry> supplier) {
        SamplerRegistry r = supplier.get();
        try (TimeSampler planSampler = r.createSampler(Metrics.PLAN_MS, listener);
             TimeSampler optSampler = r.createSampler(Metrics.OPT_MS, listener)) {
            assertEquals(new HashSet<>(r.getCurrentThreadSamplers()),
                         Sets.newHashSet(planSampler, optSampler));
        }
        assertEquals(new HashSet<>(r.getCurrentThreadSamplers()), Collections.emptySet());
    }

    @Test(dataProvider = "suppliersData")
    public void testAutoStopResumeSamplers(Supplier<SamplerRegistry> supplier) throws InterruptedException {
        SamplerRegistry r = supplier.get();
        assertTrue(Metrics.OPT_MS.isContainedBy(Metrics.PLAN_MS));
        assertTrue(Metrics.PLAN_MS.contains(Metrics.OPT_MS));
        assertFalse(Metrics.PRE_PLAN_MS.contains(Metrics.PLAN_MS));
        assertFalse(Metrics.PRE_PLAN_MS.contains(Metrics.OPT_MS));

        try (TimeSampler outSampler = r.createSampler(Metrics.PRE_PLAN_MS, listener)) {
            Thread.sleep(200);
            try (TimeSampler planSampler = r.createSampler(Metrics.PLAN_MS, listener)) {
                Thread.sleep(200);
                try (TimeSampler optSampler = r.createSampler(Metrics.OPT_MS, listener)) {
                    assertEquals(new HashSet<>(r.getCurrentThreadSamplers()),
                            Sets.newHashSet(outSampler, planSampler, optSampler));
                    Thread.sleep(200);
                }
                assertEquals(new HashSet<>(r.getCurrentThreadSamplers()),
                             Sets.newHashSet(outSampler, planSampler));
            }
            assertEquals(new HashSet<>(r.getCurrentThreadSamplers()), singleton(outSampler));
        }
        assertEquals(new HashSet<>(r.getCurrentThreadSamplers()), emptySet());

        listener.sync();
        assertEquals(listener.getValues(Metrics.OPT_MS).size(), 1); // should be ~200
        assertTrue(listener.getValues(Metrics.OPT_MS).get(0) <=  300);

        assertEquals(listener.getValues(Metrics.PLAN_MS).size(), 1); // should be ~400
        assertTrue(listener.getValues(Metrics.PLAN_MS).get(0) <=  500);
        assertTrue(listener.getValues(Metrics.PLAN_MS).get(0) >=  350);

        assertEquals(listener.getValues(Metrics.PRE_PLAN_MS).size(), 1); // should be ~200
        assertTrue(listener.getValues(Metrics.PRE_PLAN_MS).get(0) <=  300);
    }
}