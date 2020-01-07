package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;

import javax.annotation.Nonnull;

public class MismatchingQueryException extends RuntimeException {
    private @Nonnull CQuery query;
    private @Nonnull WebAPICQEndpoint endpoint;

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
