package br.ufsc.lapesd.freqel.federation.spec;

import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerFederationComponent;
import br.ufsc.lapesd.freqel.federation.inject.dagger.FederationComponent;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoader;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoaderRegistry;
import br.ufsc.lapesd.freqel.util.DictTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FederationSpecLoader {
    private static final Logger logger = LoggerFactory.getLogger(FederationSpecLoader.class);
    private @Nullable List<FreqelConfig> layers = null;

    public @Nonnull FederationSpecLoader addConfig(@Nonnull FreqelConfig config) {
        if (layers == null)
            layers = new ArrayList<>();
        layers.add(config);
        return this;
    }

    public @Nonnull FederationSpecLoader clearConfigLayers() {
        layers = null;
        return this;
    }

    public @Nonnull Federation load(@Nonnull File file) throws IOException, FederationSpecException {
        DictTree tree = DictTree.load().fromFile(file);
        return load(tree, file.getParentFile());
    }

    public @Nonnull Federation load(@Nonnull DictTree spec,
                                    @Nonnull File reference) throws FederationSpecException {
        List<Object> list = spec.getListNN("sources");
        if (list.isEmpty())
            throw new FederationSpecException("No sources listed!", spec);
        FederationComponent component = createComponent(spec, reference);
        SourceLoaderRegistry loaders = component.sourceLoaders();
        Federation federation = component.federation();
        for (Object obj : list) {
            if (!(obj instanceof DictTree)) {
                logger.warn("Ignoring non-object entry {} in sources list", obj);
                continue;
            }
            DictTree srcSpec = (DictTree) obj;
            SourceLoader loader = loaders.getLoaderFor(srcSpec);
            loader.load(srcSpec, reference).forEach(federation::addSource);
        }
        return federation;
    }

    private @Nonnull FederationComponent
    createComponent(@Nonnull DictTree spec, @Nonnull File refDir) throws FederationSpecException {
        FederationComponent.Builder b = DaggerFederationComponent.builder();
        FreqelConfig config = FreqelConfig.createDefault();
        if (layers != null)
            layers.forEach(config::readFrom);
        try {
            config.readFrom(spec, refDir);
        } catch (FreqelConfig.InvalidValueException e) {
            throw new FederationSpecException("Bad config value: "+e.getMessage(), e, spec);
        }
        b.overrideFreqelConfig(config);
        return b.build();
    }
}
