package br.ufsc.lapesd.freqel.reason.tbox;

import javax.annotation.Nonnull;

public class ReasonerException extends RuntimeException {
    public ReasonerException(@Nonnull String message) {
        super(message);
    }

    public ReasonerException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    public ReasonerException(@Nonnull Throwable cause) {
        super(cause);
    }
}
