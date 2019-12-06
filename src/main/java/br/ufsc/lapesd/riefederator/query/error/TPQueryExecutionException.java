package br.ufsc.lapesd.riefederator.query.error;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;

import javax.annotation.Nonnull;

public class TPQueryExecutionException extends QueryExecutionException {
    private final @Nonnull Triple triplePattern;
    private final @Nonnull TPEndpoint endpoint;

    public TPQueryExecutionException(@Nonnull Triple triplePattern, @Nonnull TPEndpoint endpoint) {
        this("Error processing TP "+ triplePattern +" on "+endpoint, triplePattern, endpoint);
    }

    public TPQueryExecutionException(@Nonnull String message, @Nonnull Triple triplePattern,
                                     @Nonnull TPEndpoint endpoint) {
        super(message);
        this.triplePattern = triplePattern;
        this.endpoint = endpoint;
    }

    public TPQueryExecutionException(@Nonnull String message, @Nonnull Throwable cause,
                                     @Nonnull Triple triplePattern, @Nonnull TPEndpoint endpoint) {
        super(message, cause);
        this.triplePattern = triplePattern;
        this.endpoint = endpoint;
    }

    public TPQueryExecutionException(@Nonnull Throwable cause, @Nonnull Triple triplePattern,
                                     @Nonnull TPEndpoint endpoint) {
        this(cause.getMessage() + ". While processing "+ triplePattern +" on "+endpoint, cause,
                triplePattern, endpoint);
    }

    public @Nonnull Triple getTriplePattern() {
        return triplePattern;
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }
}
