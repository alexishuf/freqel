package br.ufsc.lapesd.freqel.webapis.requests.impl;

import javax.annotation.Nonnull;

public class APIRequestExecutorException extends RuntimeException {
    public APIRequestExecutorException(@Nonnull String message) {
        super(message);
    }

    public APIRequestExecutorException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }
}
