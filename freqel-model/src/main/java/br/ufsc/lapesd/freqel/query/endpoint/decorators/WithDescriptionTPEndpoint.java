package br.ufsc.lapesd.freqel.query.endpoint.decorators;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.results.Results;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

public class WithDescriptionTPEndpoint extends AbstractTPEndpointDecorator implements TPEndpoint {
    private final @Nonnull TPEndpoint delegate;
    private final @Nonnull Description description;

    public WithDescriptionTPEndpoint(@Nonnull TPEndpoint delegate,
                                     @Nonnull Description description) {
        super(delegate);
        this.delegate = delegate;
        this.description = description;
    }

    @Override public @Nonnull Description getDescription() {
        return description;
    }

    @Override public String toString() {
        return String.format("with(%s, %s)", getDescription(), delegate);
    }

    /* --- --- --- Plain delegates --- --- --- */

    @Override @Nonnull @Contract("_ -> new") public Results query(@Nonnull Triple query) {
        return delegate.query(query);
    }

    @Override @Nonnull @Contract("_ -> new") public Results query(@Nonnull CQuery query) {
        return delegate.query(query);
    }

    @Override @Nonnull public Cardinality estimate(@Nonnull CQuery query, int estimatePolicy) {
        return delegate.estimate(query, estimatePolicy);
    }

    @Override @Nonnull public Cardinality estimate(@Nonnull CQuery query) {
        return delegate.estimate(query);
    }

    @Override public boolean isWebAPILike() {
        return delegate.isWebAPILike();
    }

    @Override @Nonnull public Set<TPEndpoint> getAlternatives() {
        return delegate.getAlternatives();
    }

    @Override @Nonnull public Set<TPEndpoint> getAlternativesClosure() {
        return delegate.getAlternativesClosure();
    }

    @Override public boolean isAlternative(@Nonnull TPEndpoint other) {
        return delegate.isAlternative(other);
    }

    @Override public void addAlternatives(Collection<? extends TPEndpoint> alternatives) {
        delegate.addAlternatives(alternatives);
    }

    @Override public void addAlternative(@Nonnull TPEndpoint alternative) {
        delegate.addAlternative(alternative);
    }

    @Override public boolean hasCapability(@Nonnull Capability capability) {
        return delegate.hasCapability(capability);
    }

    @Override public boolean hasSPARQLCapabilities() {
        return delegate.hasSPARQLCapabilities();
    }

    @Override public boolean hasRemoteCapability(@Nonnull Capability capability) {
        return delegate.hasRemoteCapability(capability);
    }

    @Override public boolean requiresBindWithOverride() {
        return delegate.requiresBindWithOverride();
    }

    @Override public boolean ignoresAtoms() {
        return delegate.ignoresAtoms();
    }

    @Override public double alternativePenalty(@Nonnull CQuery query) {
        return delegate.alternativePenalty(query);
    }

    @Override public void close() {
        delegate.close();
    }
}