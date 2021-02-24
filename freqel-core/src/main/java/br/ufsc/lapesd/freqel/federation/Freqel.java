package br.ufsc.lapesd.freqel.federation;

import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerFederationComponent;
import br.ufsc.lapesd.freqel.federation.inject.dagger.FederationComponent;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecException;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;

public class Freqel {
    public static @Nonnull Federation createFederation() {
        return DaggerFederationComponent.builder().build().federation();
    }

    public static @Nonnull Federation createFederation(File specFile)
            throws IOException, FederationSpecException {
        FederationSpecLoader loader = new FederationSpecLoader();
        return loader.load(specFile);
    }

    public static @Nonnull Federation createFederation(@Nonnull TPEndpoint ep) {
        Federation federation = createFederation();
        federation.addSource(ep);
        return federation;
    }

    public static @Nonnull Federation createFederation(@Nonnull FreqelConfig config) {
        return DaggerFederationComponent.builder().overrideFreqelConfig(config).build()
                                        .federation();
    }

    public static @Nonnull Federation createFederation(@Nonnull FreqelConfig config,
                                                       @Nonnull TPEndpoint ep) {
        Federation federation = createFederation(config);
        federation.addSource(ep);
        return federation;
    }

    public static @Nonnull FederationComponent.Builder createFederationBuilder() {
        return DaggerFederationComponent.builder();
    }
}
