package br.ufsc.lapesd.riefederator.query.parse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SPARQLParseException extends Exception {
    private @Nullable String query;

    public SPARQLParseException(@Nonnull String message, @Nullable String query) {
        super(message);
        this.query = query;
    }

    public SPARQLParseException(@Nonnull String message, @Nonnull Throwable cause,
                                @Nullable String query) {
        super(message, cause);
        this.query = query;
    }

    public @Nullable String getQuery() {
        return query;
    }
}
