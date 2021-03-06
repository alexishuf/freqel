package br.ufsc.lapesd.freqel.federation.spec.source;

import br.ufsc.lapesd.freqel.util.DictTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class SourceLoaderRegistry {
    private final static Logger logger = LoggerFactory.getLogger(SourceLoaderRegistry.class);
    private final @Nonnull Map<String, SourceLoader> loaders;

    public SourceLoaderRegistry() {
        this(new HashMap<>());
    }

    public SourceLoaderRegistry(@Nonnull Map<String, SourceLoader> loaders) {
        this.loaders = loaders;
    }

    @VisibleForTesting @Nonnull Map<String, SourceLoader> getLoaders() {
        return loaders;
    }

    @CheckReturnValue
    public @Nullable SourceLoader get(@Nonnull String name) {
        return loaders.get(name.trim().toLowerCase());
    }

    public @Nonnull SourceLoader getLoaderFor(@Nonnull DictTree tree) throws SourceLoadException {
        String name = tree.getString("loader", "").trim().toLowerCase();
        SourceLoader loader = get(name);
        if (loader == null)
            throw new SourceLoadException("No SourceLoader for "+name, tree);
        return loader;
    }

    public @Nonnull Collection<SourceLoader> getSourceLoaders() {
        return Collections.unmodifiableCollection(loaders.values());
    }

    @Contract("-> this") @CanIgnoreReturnValue
    public @Nonnull SourceLoaderRegistry registerAllSPIs() {
        ServiceLoader<SourceLoader> svcLoader = ServiceLoader.load(SourceLoader.class);
        for (SourceLoader sourceLoader : svcLoader)
            register(sourceLoader);
        return this;
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull SourceLoaderRegistry register(@Nonnull SourceLoader loader) {
        for (String name : loader.names()) {
            String key = name.trim().toLowerCase();
            SourceLoader old = loaders.get(key);
            if (old != null && old != loader) {
                logger.warn("There already is a SourceLoader {} registered for name {}. " +
                            "Will not register {}", old, name, loader);
            } else {
                loaders.put(key, loader);
            }
        }
        return this;
    }


}
