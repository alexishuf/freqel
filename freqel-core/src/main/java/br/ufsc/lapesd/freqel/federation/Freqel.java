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

    public static @Nonnull Federation createFederation(@Nonnull File specFile)
            throws IOException, FederationSpecException {
        FederationSpecLoader loader = new FederationSpecLoader();
        return loader.load(specFile);
    }

    public static @Nonnull Federation createFederation(@Nonnull TPEndpoint... eps) {
        Federation federation = createFederation();
        for (TPEndpoint ep : eps) federation.addSource(ep);
        return federation;
    }

    public static @Nonnull Federation createFederation(@Nonnull FreqelConfig config) {
        return DaggerFederationComponent.builder().overrideFreqelConfig(config).build()
                                        .federation();
    }

    public static @Nonnull Federation createFederation(@Nonnull FreqelConfig config,
                                                       @Nonnull TPEndpoint... eps) {
        Federation federation = createFederation(config);
        for (TPEndpoint ep : eps)
            federation.addSource(ep);
        return federation;
    }

    public static @Nonnull FederationComponent.Builder builder() {
        return DaggerFederationComponent.builder();
    }
}
