package br.ufsc.lapesd.riefederator.query.endpoint;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

public enum  Capability {
    ASK,
    PROJECTION,
    DISTINCT,
    LIMIT,
    SPARQL_FILTER,
    VALUES;

    @CanIgnoreReturnValue
    public @Nonnull TPEndpoint requireFrom(@Nonnull TPEndpoint ep) throws MissingCapabilityException {
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
