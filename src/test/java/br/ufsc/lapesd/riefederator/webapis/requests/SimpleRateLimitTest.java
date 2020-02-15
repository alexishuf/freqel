package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimit;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.impl.SimpleRateLimit;
import com.google.common.base.Stopwatch;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.*;
import static org.testng.Assert.assertFalse;

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
}