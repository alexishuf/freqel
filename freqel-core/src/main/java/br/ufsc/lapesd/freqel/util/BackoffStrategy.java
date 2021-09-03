package br.ufsc.lapesd.freqel.util;

import javax.annotation.Nonnull;

public interface BackoffStrategy {
    /**
     * Checks if {@link BackoffStrategy#backOff()} would return true, without risk being blocked.
     *
     * @return true iff {@link BackoffStrategy#backOff()} also would.
     */
    boolean canRetry();

    /**
     * Block the calling thread until a new retry is allowed or return false immediately.
     *
     * @return true iff a new try should be done, false if no retries should be done.
     */
    boolean backOff();

    /**
     * Schedule a call to onRetry after a backoff period or return false if no retry should be made.
     *
     * @param onRetry callback to be called by another thread, only if this method returns true.
     * @return true iff a retry should be made when onRetry is called.
     */
    boolean backOff(@Nonnull Runnable onRetry);

    void reset();

    /**
     * Create a new {@link BackoffStrategy} instance with the same initial parameters.
     *
     * The current state of this instance (i.e., the number of previous calls to one
     * of the backOff methods) does not affect the result of this method.
     *
     * @return a new {@link BackoffStrategy} instance implementing the same strategy
     * from the same initial parameters.
     */
    @Nonnull BackoffStrategy create();
}
