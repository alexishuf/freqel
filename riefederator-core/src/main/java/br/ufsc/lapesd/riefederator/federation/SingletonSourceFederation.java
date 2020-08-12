package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import com.google.inject.Guice;
import com.google.inject.Injector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SingletonSourceFederation extends SimpleFederationModule {
    private static @Nullable Injector injector;

    public static @Nonnull Injector getInjector() {
        if (injector == null)
            injector = Guice.createInjector(new SingletonSourceFederation());
        return injector;
    }

    public static @Nonnull Federation createFederation(@Nonnull Source source) {
        Federation federation = getInjector().getInstance(Federation.class);
        federation.addSource(source);
        return federation;
    }

    @Override
    protected void configureResultsExecutor() {
        bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
    }
}
