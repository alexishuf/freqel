package br.ufsc.lapesd.freqel.query.endpoint.exceptions;

import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class QueryExecutionException extends RuntimeException {
    private TPEndpoint ep;

    public QueryExecutionException(String message) {
        super(message);
    }

    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryExecutionException(Throwable cause) {
        super(cause);
    }

    public QueryExecutionException(@Nonnull TPEndpoint ep, @Nonnull String message) {
        super(ep+": "+message);
    }

    public QueryExecutionException(@Nonnull TPEndpoint ep, @Nonnull Throwable t) {
        super(ep+": "+t.getMessage(), t instanceof QueryExecutionException ? t.getCause() : t);
    }

    public static @Nonnull QueryExecutionException wrap(@Nonnull Throwable t,
                                                        @Nullable TPEndpoint ep) {
        if (t instanceof QueryExecutionException) {
            QueryExecutionException q = (QueryExecutionException) t;
            if (ep != null)
                return new QueryExecutionException(ep+": "+q.getMessage(), q.getCause());
            return q;
        }
        if (ep != null)
            return new QueryExecutionException(ep, t);
        return new QueryExecutionException(t);
    }
}
