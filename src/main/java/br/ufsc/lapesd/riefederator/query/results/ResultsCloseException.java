package br.ufsc.lapesd.riefederator.query.results;

import javax.annotation.Nonnull;

/**
 * A {@link RuntimeException} for use in {@link Results}.close()
 */
public class ResultsCloseException extends RuntimeException {
    private final @Nonnull Results results;

    public ResultsCloseException(@Nonnull Results results, @Nonnull String message, Throwable cause) {
        super(message, cause);
        this.results = results;
    }

    public ResultsCloseException(@Nonnull Results results, Throwable cause) {
        this(results, cause.getClass().getSimpleName()+" when close()ing "+results, cause);
    }

    public @Nonnull Results getResults() {
        return results;
    }
}
