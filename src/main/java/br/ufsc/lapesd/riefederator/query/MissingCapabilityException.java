package br.ufsc.lapesd.riefederator.query;

import javax.annotation.Nonnull;

public class MissingCapabilityException extends RuntimeException {
    private @Nonnull Capability capability;
    private @Nonnull TPEndpoint endpoint;

    public MissingCapabilityException(@Nonnull Capability cap,
                                      @Nonnull TPEndpoint ep) {
        this(cap, ep, "Endpoint "+ep+" is missing required capability "+cap);
    }

    public MissingCapabilityException(@Nonnull Capability capability, @Nonnull TPEndpoint endpoint,
                                      @Nonnull String message) {
        super(message);
        this.capability = capability;
        this.endpoint = endpoint;
    }

    public @Nonnull Capability getCapability() {
        return capability;
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }
}
