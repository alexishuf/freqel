package br.ufsc.lapesd.freqel.util;

import com.google.common.base.Stopwatch;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

public class ExponentialBackoffTest {

    @Test(groups = {"fast"}, timeOut = 500)
    public void testNeverBackoff() throws InterruptedException {
        AtomicInteger retries = new AtomicInteger(0);
        BackoffStrategy backoff = ExponentialBackoff.spamming(4);

        for (int i = 0; i < 3; i++) {
            assertTrue(backoff.backOff(), "i="+i);
            assertTrue(backoff.backOff(retries::incrementAndGet), "i="+i);
            assertTrue(backoff.backOff(), "i="+i);
            assertTrue(backoff.backOff(retries::incrementAndGet), "i="+i);
            assertFalse(backoff.backOff(), "i="+i);
            assertFalse(backoff.backOff(retries::incrementAndGet), "i="+i);
            Thread.yield();
            if (i == 0)
                backoff.reset();
            else if (i == 1)
                backoff = backoff.create();
        }
        assertFalse(backoff.backOff());
        assertFalse(backoff.backOff(retries::incrementAndGet));

        Thread.sleep(100);
        assertEquals(retries.get(), 3*2);
    }

    @Test(groups = {"fast"}, timeOut = 500)
    public void testNeverRetry() {
        boolean[] failed = {false};
        Runnable fail = () -> failed[0] = true;
        assertFalse(ExponentialBackoff.neverRetry().backOff());
        assertFalse(ExponentialBackoff.neverRetry().backOff(fail));
        assertFalse(new ExponentialBackoff(10000, 0).backOff());
        assertFalse(new ExponentialBackoff(10000, 0).backOff(fail));
        assertFalse(failed[0]);
    }

    @Test
    public void testExponentialBackoff() throws InterruptedException, ExecutionException {
        ExponentialBackoff backoff = new ExponentialBackoff(200, 3);

        Stopwatch sw = Stopwatch.createStarted();
        assertTrue(backoff.backOff()); // 200
        long elapsed = sw.elapsed(TimeUnit.MILLISECONDS);
        assertTrue(elapsed > 100, "elapsed="+elapsed+", expected >100 (~200)");
        assertTrue(elapsed < 400, "elapsed="+elapsed+", expected <400 (~200)");

        int ex = 400;
        for (int i = 0; i < 2; i++) {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            assertTrue(backoff.backOff(() -> future.complete(23)));
            sw.reset().start();
            assertEquals(future.get(), Integer.valueOf(23));
            elapsed = sw.elapsed(TimeUnit.MILLISECONDS);
            assertTrue(elapsed > ex-100, "elapsed="+elapsed+" too short, expected ~"+ex);
            assertTrue(elapsed < ex+200, "elapsed="+elapsed+ " too long, expected ~"+ex);
            ex *= 2;
        }

        boolean[] fail = {false};
        assertFalse(backoff.backOff(() -> fail[0] = true));
        assertFalse(fail[0]);
    }

}