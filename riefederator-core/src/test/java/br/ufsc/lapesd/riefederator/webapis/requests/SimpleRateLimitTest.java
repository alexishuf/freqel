package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimit;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.impl.SimpleRateLimit;
import com.google.common.base.Stopwatch;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SimpleRateLimitTest {

    @DataProvider
    public static Object[][] rateData() {
        Supplier<RateLimit> supp0, supp1, supp2;
        supp0 = () -> SimpleRateLimit.perInterval(10, 1, SECONDS);
        supp1 = () -> SimpleRateLimit.perInterval(90, 1, MINUTES);
        supp2 = () -> SimpleRateLimit.perInterval(10, 1, SECONDS,
                                    100, MILLISECONDS);
        return Stream.of(
                asList(supp0, 5000, 40, 51),
                asList(supp1, 5000, (int)Math.ceil(90*(5/60.0))-10, (int)Math.ceil(90*(5/60.0))),
                asList(supp2, 5000, 20, 49)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "rateData")
    public void testRate(Supplier<RateLimit> supplier, int milliseconds,
                         int min, int max) throws Exception {
        RateLimit limit = supplier.get();
        AtomicInteger calls = new AtomicInteger(0);
        Stopwatch sw = Stopwatch.createStarted();
        while (sw.elapsed(MILLISECONDS) < milliseconds)
            limit.request(calls::incrementAndGet);
        assertFalse(calls.get() < min, "Sleeping too much! calls="+calls);
        assertFalse(calls.get() > max, "Not sleeping enough! calls="+calls);
    }

    @Test
    public void testSlowCallable() throws ExecutionException, InterruptedException {
        int sleepMs = 100, taskMs = 500;
        SimpleRateLimit limit = new SimpleRateLimit(sleepMs);
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<>();
        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < 8; i++) {
            futures.add(executor.submit(() -> limit.request(() -> {
                Thread.sleep(taskMs);
                return taskMs;
            })));
        }
        for (Future<?> future : futures)
            future.get(); //rethrows callable exceptions
        long elapsed = sw.elapsed(MILLISECONDS);
        int expected = 8 * (taskMs + sleepMs) - 200;
        assertTrue(elapsed > expected, "elapsed="+elapsed+", expected="+expected);
    }
}