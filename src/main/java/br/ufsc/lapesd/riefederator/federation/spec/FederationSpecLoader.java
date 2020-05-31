package br.ufsc.lapesd.riefederator.federation.spec;

import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.SimpleFederationModule;
import br.ufsc.lapesd.riefederator.federation.spec.source.SourceLoader;
import br.ufsc.lapesd.riefederator.federation.spec.source.SourceLoaderRegistry;
import br.ufsc.lapesd.riefederator.query.EstimatePolicy;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.inject.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.inject.Guice.createInjector;

public class FederationSpecLoader {
    private static final Logger logger = LoggerFactory.getLogger(FederationSpecLoader.class);

    private @Nonnull SourceLoaderRegistry loaderRegistry = SourceLoaderRegistry.getDefault();
    private @Nonnull Module federationComponents = new SimpleFederationModule();

    public void setLoaderRegistry(@Nonnull SourceLoaderRegistry loaderRegistry) {
        this.loaderRegistry = loaderRegistry;
    }

    public @Nonnull SourceLoaderRegistry getLoaderRegistry() {
        return loaderRegistry;
    }

    public @Nonnull Module getFederationComponents() {
        return federationComponents;
    }

    public void setFederationComponents(@Nonnull Module federationComponents) {
        this.federationComponents = federationComponents;
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
        Federation federation = createInjector(federationComponents).getInstance(Federation.class);
        federation.setEstimatePolicy(parseEstimatePolicy(spec));
        for (Object obj : list) {
            if (!(obj instanceof DictTree)) {
                logger.warn("Ignoring non-object entry {} in sources list", obj);
                continue;
            }
            DictTree srcSpec = (DictTree) obj;
            SourceLoader loader = loaderRegistry.getLoaderFor(srcSpec);
            loader.load(srcSpec, reference).forEach(federation::addSource);
        }
        return federation;
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
