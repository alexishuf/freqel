package br.ufsc.lapesd.freqel.query.endpoint.decorators;

import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;
import java.util.Collection;

public class WithDescriptionCQEndpoint extends WithDescriptionTPEndpoint implements CQEndpoint {
    private final @Nonnull CQEndpoint delegate;

    public WithDescriptionCQEndpoint(@Nonnull CQEndpoint delegate,
                                     @Nonnull Description description) {
        super(delegate, description);
        this.delegate = delegate;
    }

    @Override public boolean canQuerySPARQL() {
        return delegate.canQuerySPARQL();
    }

    @Override public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery) {
        return delegate.querySPARQL(sparqlQuery);
    }

    @Override public @Nonnull Results
    querySPARQL(@Nonnull String sparqlQuery, boolean isAsk, @Nonnull Collection<String> varNames) {
        return delegate.querySPARQL(sparqlQuery, isAsk, varNames);
    }
}
