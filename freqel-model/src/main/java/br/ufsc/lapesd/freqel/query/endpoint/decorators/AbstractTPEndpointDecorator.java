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

public abstract class AbstractTPEndpointDecorator implements TPEndpoint {
    protected final @Nonnull TPEndpoint delegate;

    public AbstractTPEndpointDecorator(@Nonnull TPEndpoint delegate) {
        this.delegate = delegate;
    }

    @Override public @Nonnull TPEndpoint getEffective() {
        return delegate.getEffective();
    }

    @Override public boolean isAlternative(@Nonnull TPEndpoint other) {
        return getEffective().isAlternative(other.getEffective());
    }

    /* --- --- --- plain delegates --- --- --- */

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

    @Override @Nonnull public Description getDescription() {
        return delegate.getDescription();
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
