package br.ufsc.lapesd.riefederator.webapis.requests;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

public class NoRateLimit implements RateLimit {
    public static final @Nonnull NoRateLimit INSTANCE = new NoRateLimit();

    @Override
    public void request(@Nonnull Runnable runnable) {
        runnable.run();
    }

    @Override
    public <V> V request(@Nonnull Callable<V> callable) throws Exception {
        return callable.call();
    }
}
