package br.ufsc.lapesd.riefederator.query;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

public enum  Capability {
    ASK,
    PROJECTION,
    DISTINCT,
    SPARQL_FILTER;

    @CanIgnoreReturnValue
    public @Nonnull TPEndpoint
    requireFrom(@Nonnull TPEndpoint ep) throws MissingCapabilityException {
        if (!ep.hasCapability(this))
            throw new MissingCapabilityException(this, ep);
        return ep;
    }

    @CanIgnoreReturnValue
    public @Nonnull TPEndpoint
    requireFromRemote(@Nonnull TPEndpoint ep) throws MissingCapabilityException {
        if (!ep.hasRemoteCapability(this))
            throw new MissingCapabilityException(this, ep);
        return ep;
    }
}
