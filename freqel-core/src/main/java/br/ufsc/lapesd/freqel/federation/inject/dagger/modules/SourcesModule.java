package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoader;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoaderRegistry;
import br.ufsc.lapesd.freqel.util.BackoffStrategy;
import br.ufsc.lapesd.freqel.util.ExponentialBackoff;
import dagger.Module;
import dagger.Provides;
import dagger.Reusable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static java.util.Objects.requireNonNull;

@Module
public abstract class SourcesModule {

    @Provides @Singleton public static SourceLoaderRegistry
    sourceLoaderRegistry(@Named("tempDir") File tempDir,
                         SourceCache sourceCache,
                         @Named("indexing") BackoffStrategy indexingBackoffStrategy) {
        SourceLoaderRegistry registry = new SourceLoaderRegistry().registerAllSPIs();
        for (SourceLoader loader : registry.getSourceLoaders()) {
            loader.setTempDir(tempDir);
            loader.setSourceCache(sourceCache);
            loader.setIndexingBackoffStrategy(indexingBackoffStrategy);
        }
        return registry;
    }

    @Provides public static @Nonnull @Named("trustSourceCache") Boolean
    trustSourceCache(@Nullable @Named("overrideTrustSourceCache") Boolean override,
                     @Nonnull FreqelConfig config) {
        return override != null ? override : config.get(TRUST_SOURCE_CACHE, Boolean.class);
    }

    @Provides @Reusable public static @Named("sourceCacheDir") File
    sourceCacheDir(@Named("sourceCacheDirOverride") @Nullable File dir, FreqelConfig config) {
        return dir != null ? dir : requireNonNull(config.get(SOURCES_CACHE_DIR, File.class));
    }

    @Provides @Singleton public static SourceCache
    sourcesCache(@Named("override") @Nullable SourceCache override,
                 @Named("sourceCacheDir") File dir, @Named("trustSourceCache") Boolean trustCache) {
        SourceCache instance = override != null ? override : new SourceCache(dir, trustCache);
        try {
            instance.loadIndex();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load SourceCache", e);
        }
        return instance;
    }

    @Provides public static @Nonnull @Named("indexing") ExponentialBackoff
    indexingExponentialBackoff(@Nonnull FreqelConfig config) {
        Integer ms = config.get(INDEXING_FIRST_BACKOFF_MILLISECONDS, Integer.class);
        Integer maxCount = config.get(INDEXING_BACKOFF_MAX_COUNT, Integer.class);
        return new ExponentialBackoff(ms, maxCount);
    }

    @Provides @Nonnull public static @Named("indexing") BackoffStrategy
    indexingBackoffStrategy(@Named("indexingOverride") @Nullable BackoffStrategy override,
                            @Nonnull FreqelConfig config,
                            @Nonnull @Named("indexing") Provider<ExponentialBackoff> expProvider) {
        if (override != null)
            return override;
        String expName = ExponentialBackoff.class.getName();
        String name = config.get(INDEXING_BACKOFF_STRATEGY, String.class);
        if (name.equalsIgnoreCase(expName) || expName.toLowerCase().endsWith(name))
            return expProvider.get();
        return ModuleHelper.get(BackoffStrategy.class, name);
    }
}
