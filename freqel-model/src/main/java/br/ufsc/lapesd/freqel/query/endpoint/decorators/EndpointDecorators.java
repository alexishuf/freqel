package br.ufsc.lapesd.freqel.query.endpoint.decorators;

import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;

@SuppressWarnings("unchecked")
public class EndpointDecorators {
    public static @Nonnull <T extends TPEndpoint> T uncloseable(@Nonnull T ep) {
        if (ep instanceof DQEndpoint)
            return (T)new UncloseableDQEndpoint((DQEndpoint) ep);
        else if (ep instanceof CQEndpoint)
            return (T)new UncloseableCQEndpoint((CQEndpoint) ep);
        else
            return (T)new UncloseableTPEndpoint(ep);
    }

    public static @Nonnull <T extends TPEndpoint> T
    withDescription(@Nonnull T ep, @Nonnull Description description) {
        if (ep instanceof DQEndpoint)
            return (T)new WithDescriptionDQEndpoint((DQEndpoint) ep, description);
        else if (ep instanceof CQEndpoint)
            return (T)new WithDescriptionCQEndpoint((CQEndpoint) ep, description);
        else
            return (T)new WithDescriptionTPEndpoint(ep, description);
    }
}
