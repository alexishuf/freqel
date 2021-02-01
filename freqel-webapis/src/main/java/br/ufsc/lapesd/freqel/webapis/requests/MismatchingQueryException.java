package br.ufsc.lapesd.freqel.webapis.requests;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.webapis.WebAPICQEndpoint;

import javax.annotation.Nonnull;

public class MismatchingQueryException extends RuntimeException {
    private final @Nonnull CQuery query;
    private final @Nonnull WebAPICQEndpoint endpoint;

    public MismatchingQueryException(@Nonnull CQuery query, @Nonnull WebAPICQEndpoint endpoint) {
        super(String.format("Query %s does not match with the endpoint %s", query, endpoint));
        this.query = query;
        this.endpoint = endpoint;
    }

    public @Nonnull CQuery getQuery() {
        return query;
    }

    public @Nonnull WebAPICQEndpoint getEndpoint() {
        return endpoint;
    }
}
