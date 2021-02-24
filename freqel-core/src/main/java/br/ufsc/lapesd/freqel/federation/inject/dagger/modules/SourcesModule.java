package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoader;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoaderRegistry;
import dagger.Module;
import dagger.Provides;
import dagger.Reusable;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.SOURCES_CACHE_DIR;
import static java.util.Objects.requireNonNull;

@Module
public abstract class SourcesModule {

    @Provides @Singleton public static SourceLoaderRegistry
    sourceLoaderRegistry(@Named("tempDir") File tempDir, SourceCache sourceCache) {
        SourceLoaderRegistry registry = new SourceLoaderRegistry().registerAllSPIs();
        for (SourceLoader loader : registry.getSourceLoaders()) {
            loader.setTempDir(tempDir);
            loader.setSourceCache(sourceCache);
        }
        return registry;
    }

    @Provides @Reusable public static @Named("sourceCacheDir") File
    sourceCacheDir(@Named("sourceCacheDirOverride") @Nullable File dir, FreqelConfig config) {
        return dir != null ? dir : requireNonNull(config.get(SOURCES_CACHE_DIR, File.class));
    }

    @Provides @Singleton public static SourceCache
    sourcesCache(@Named("override") @Nullable SourceCache override,
                 @Named("sourceCacheDir") File dir) {
        SourceCache instance = override != null ? override : new SourceCache(dir);
        try {
            instance.loadIndex();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load SourceCache", e);
        }
        return instance;
    }
}
