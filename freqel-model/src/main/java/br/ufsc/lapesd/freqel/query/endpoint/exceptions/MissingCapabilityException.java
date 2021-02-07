package br.ufsc.lapesd.freqel.query.endpoint.exceptions;

import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;

public class MissingCapabilityException extends RuntimeException {
    private final @Nonnull Capability capability;
    private final @Nonnull TPEndpoint endpoint;

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
