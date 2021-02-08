package br.ufsc.lapesd.freqel.query.endpoint.decorators;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.DisjunctiveProfile;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.DQEndpointException;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.QueryExecutionException;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public class WithDescriptionDQEndpoint extends WithDescriptionCQEndpoint implements DQEndpoint {
    private final @Nonnull DQEndpoint delegate;

    public WithDescriptionDQEndpoint(@Nonnull DQEndpoint delegate,
                                     @Nonnull Description description) {
        super(delegate, description);
        this.delegate = delegate;
    }

    @Override public @Nonnull DisjunctiveProfile getDisjunctiveProfile() {
        return delegate.getDisjunctiveProfile();
    }

    @Override public @Nonnull Results
    query(@Nonnull Op query) throws DQEndpointException, QueryExecutionException {
        return delegate.query(query);
    }
}
