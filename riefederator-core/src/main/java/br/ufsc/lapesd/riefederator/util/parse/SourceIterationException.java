package br.ufsc.lapesd.riefederator.util.parse;

import javax.annotation.Nonnull;

public class SourceIterationException extends RuntimeException {
    private @Nonnull Object source;

    public SourceIterationException(@Nonnull Object source, String msg) {
        super(msg);
        this.source = source;
    }

    public SourceIterationException(@Nonnull Object source, String msg, @Nonnull Throwable cause) {
        super(msg, cause);
        this.source = source;
    }

    public SourceIterationException(@Nonnull Object source, @Nonnull Throwable cause) {
        super(cause);
        this.source = source;
    }

    public @Nonnull Object getSource() {
        return source;
    }
}
