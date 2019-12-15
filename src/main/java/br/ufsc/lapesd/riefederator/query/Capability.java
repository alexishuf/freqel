package br.ufsc.lapesd.riefederator.query;

import javax.annotation.Nonnull;

public enum  Capability {
    PROJECTION,
    DISTINCT;

    @SuppressWarnings("UnusedReturnValue")
    public @Nonnull TPEndpoint
    requireFrom(@Nonnull TPEndpoint ep) throws MissingCapabilityException {
        if (!ep.hasCapability(this))
            throw new MissingCapabilityException(this, ep);
        return ep;
    }
}
