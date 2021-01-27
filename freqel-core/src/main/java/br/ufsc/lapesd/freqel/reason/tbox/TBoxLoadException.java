package br.ufsc.lapesd.freqel.reason.tbox;

import javax.annotation.Nonnull;

public class TBoxLoadException extends RuntimeException {
    public TBoxLoadException(@Nonnull String message) {
        super(message);
    }

    public TBoxLoadException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    public TBoxLoadException(@Nonnull Throwable cause) {
        super(cause);
    }
}
