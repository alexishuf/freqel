package br.ufsc.lapesd.riefederator.webapis.requests;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

public interface RateLimit {
    void request(@Nonnull Runnable runnable);
    <V> V request(@Nonnull Callable<V> callable) throws Exception;
}
