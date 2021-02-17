package br.ufsc.lapesd.freqel.query.endpoint.decorators;

import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;

public class EndpointDecorators {
    public static @Nonnull TPEndpoint uncloseable(@Nonnull TPEndpoint ep) {
        if (ep instanceof DQEndpoint)
            return new UncloseableDQEndpoint((DQEndpoint) ep);
        else if (ep instanceof CQEndpoint)
            return new UncloseableCQEndpoint((CQEndpoint) ep);
        else
            return new UncloseableTPEndpoint(ep);
    }
    public static @Nonnull CQEndpoint uncloseable(@Nonnull CQEndpoint ep) {
        return (CQEndpoint) uncloseable((TPEndpoint) ep);
    }
    public static @Nonnull DQEndpoint uncloseable(@Nonnull DQEndpoint ep) {
        return (DQEndpoint) uncloseable((TPEndpoint) ep);
    }

    public static @Nonnull TPEndpoint
    withDescription(@Nonnull TPEndpoint ep, @Nonnull Description description) {
        if (ep instanceof DQEndpoint)
            return new WithDescriptionDQEndpoint((DQEndpoint) ep, description);
        else if (ep instanceof CQEndpoint)
            return new WithDescriptionCQEndpoint((CQEndpoint) ep, description);
        else
            return new WithDescriptionTPEndpoint(ep, description);
    }

    public static @Nonnull CQEndpoint
    withDescription(@Nonnull CQEndpoint ep, @Nonnull Description description) {
        return (CQEndpoint) withDescription((TPEndpoint) ep, description);
    }

    public static @Nonnull DQEndpoint
    withDescription(@Nonnull DQEndpoint ep, @Nonnull Description description) {
        return (DQEndpoint) withDescription((TPEndpoint) ep, description);
    }
}
