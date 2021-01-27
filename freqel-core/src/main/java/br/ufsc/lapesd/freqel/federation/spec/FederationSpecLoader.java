package br.ufsc.lapesd.freqel.federation.spec;

import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.SimpleFederationModule;
import br.ufsc.lapesd.freqel.federation.cardinality.EstimatePolicy;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoader;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoaderRegistry;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.inject.Guice.createInjector;

public class FederationSpecLoader {
    private static final Logger logger = LoggerFactory.getLogger(FederationSpecLoader.class);

    private @Nonnull SourceLoaderRegistry loaderRegistry = SourceLoaderRegistry.getDefault();
    private @Nonnull final SimpleFederationModule federationComponents = new SimpleFederationModule();
    private @Nonnull final List<Module> overridingModules = new ArrayList<>();

    public void setLoaderRegistry(@Nonnull SourceLoaderRegistry loaderRegistry) {
        this.loaderRegistry = loaderRegistry;
    }

    public @Nonnull SourceLoaderRegistry getLoaderRegistry() {
        return loaderRegistry;
    }

    @CanIgnoreReturnValue
    public @Nonnull FederationSpecLoader overrideWith(Module module) {
        overridingModules.add(module);
        return this;
    }

    public @Nonnull Federation load(@Nonnull File file)
            throws IOException, FederationSpecException {
        DictTree tree = DictTree.load().fromFile(file);
        return load(tree, file.getParentFile());
    }

    public @Nonnull Federation load(@Nonnull DictTree spec,
                                    @Nonnull File reference) throws FederationSpecException {
        List<Object> list = spec.getListNN("sources");
        if (list.isEmpty())
            throw new FederationSpecException("No sources listed!", spec);
        federationComponents.setLimitEstimatePolicy(parseEstimatePolicy(spec));
        Injector injector = createInjector(Modules.override(federationComponents)
                                                  .with(overridingModules));
        Federation federation = injector.getInstance(Federation.class);
        SourceCache cacheDir = getSourceCache(spec, reference);
        for (Object obj : list) {
            if (!(obj instanceof DictTree)) {
                logger.warn("Ignoring non-object entry {} in sources list", obj);
                continue;
            }
            DictTree srcSpec = (DictTree) obj;
            SourceLoader loader = loaderRegistry.getLoaderFor(srcSpec);
            loader.load(srcSpec, cacheDir, reference).forEach(federation::addSource);
        }
        return federation;
    }

    private @Nullable SourceCache getSourceCache(@Nonnull DictTree spec,
                                                 @Nonnull File reference) {
        String dirPath = spec.getString("sources-cache");
        if (dirPath == null) return null;
        File file = new File(dirPath);
        if (!file.isAbsolute())
            file = new File(reference, dirPath);
        return new SourceCache(file.getAbsoluteFile());
    }

    private int parseEstimatePolicy(@Nonnull DictTree spec) throws FederationSpecException {
        String prefix = "Bad value for estimate-policy/";
        DictTree estimatePolicy = spec.getMapNN("estimate-policy");
        int policy = 0;

        String localString = estimatePolicy.getString("local", "query");
        if (localString.equals("query")) {
            policy |= EstimatePolicy.CAN_QUERY_LOCAL;
        } else if (localString.equals("ask")) {
            policy |= EstimatePolicy.CAN_ASK_LOCAL;
        } else if (!localString.equals("none")) {
            throw new FederationSpecException(prefix + "local: " +localString, spec);
        }

        String remoteString = estimatePolicy.getString("remote", "ask");
        if (remoteString.equals("query")) {
            policy |= EstimatePolicy.CAN_QUERY_REMOTE;
        } else if (remoteString.equals("ask")) {
            policy |= EstimatePolicy.CAN_ASK_REMOTE;
        } else if (!remoteString.equals("none")) {
            throw new FederationSpecException(prefix + "remote: " +remoteString, spec);
        }

        long limit = estimatePolicy.getLong("limit", 40);
        if (limit < 0)
            throw new FederationSpecException(prefix + "limit: " +limit, spec);
        policy |= EstimatePolicy.limit((int)limit);
        return policy;
    }
}
