package br.ufsc.lapesd.riefederator.webapis.requests.rate.impl;

import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimit;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

public class SimpleRateLimit implements RateLimit {
    private static final Logger logger = LoggerFactory.getLogger(SimpleRateLimit.class);
    public double sleepMinMs, sleepMaxMs;
    private @Nonnull Semaphore semaphore = new Semaphore(1);
    private @Nonnull final Stopwatch idle;

    public SimpleRateLimit(double sleepMs) {
        this(sleepMs, sleepMs);

    }
    public SimpleRateLimit(double sleepMinMs, double sleepMaxMs) {
        this.sleepMinMs = sleepMinMs;
        this.sleepMaxMs = sleepMaxMs;
        this.idle = Stopwatch.createStarted();
    }

    public static @Nonnull RateLimit perInterval(int countPerInterval,
                                                 int interval, TimeUnit intervalUnit,
                                                 int randomInterval, TimeUnit randomUnit) {
        long msecs = MILLISECONDS.convert(interval, intervalUnit);
        long min = msecs / countPerInterval;
        double max = min + MILLISECONDS.convert(randomInterval, randomUnit);
        return new SimpleRateLimit(min, max);
    }

    public static @Nonnull RateLimit perInterval(int countPerInterval,
                                                 int interval, TimeUnit intervalUnit) {
        return perInterval(countPerInterval, interval, intervalUnit, 0, MILLISECONDS);
    }

    public static @Nonnull RateLimit perMinute(int requests,
                                               int randomInterval, TimeUnit randomIntervalUnit) {
        return perInterval(requests, 1, MINUTES, randomInterval, randomIntervalUnit);
    }
    public static @Nonnull RateLimit perMinute(int requests) {
        return perInterval(requests, 1, MINUTES);
    }
    public static @Nonnull RateLimit perHour(int requests,
                                             int randomInterval, TimeUnit randomIntervalUnit) {
        return perInterval(requests, 1, HOURS, randomInterval, randomIntervalUnit);
    }
    public static @Nonnull RateLimit perHour(int requests) {
        return perInterval(requests, 1, HOURS);
    }

    @Override
    public void request(@Nonnull Runnable runnable) {
        try {
            request(() -> {runnable.run(); return null;});
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
    @Override
    public <V> V request(@Nonnull Callable<V> callable) throws Exception {
        semaphore.acquire();
        try {
            sleepAsRequired();
            V result = callable.call();
            idle.reset().start();
            return result;
        } finally {
            semaphore.release();
        }
    }

    public void sleepAsRequired() {
        assert sleepMaxMs >= sleepMinMs;
        double need = sleepMinMs;
        if (sleepMinMs != sleepMaxMs)
            need = sleepMinMs + Math.random()*(sleepMaxMs-sleepMinMs);
        need -= idle.elapsed(TimeUnit.MICROSECONDS)/1000.0;
        if (need <= 0)
            return; //no sleep required
        try {
            logger.debug("Sleeping {}ms due to {}", need, this);
            Thread.sleep((int)Math.ceil(need));
        } catch (InterruptedException e) {
            logger.warn("InterruptedException during sleep of {}ms", need);
        }
    }
}
