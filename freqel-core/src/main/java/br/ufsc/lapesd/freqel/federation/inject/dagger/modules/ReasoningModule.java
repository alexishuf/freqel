package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoaderRegistry;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.*;
import br.ufsc.lapesd.freqel.reason.tbox.endpoint.HeuristicEndpointReasoner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementGenerator;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.generators.SubTermReplacementGenerator;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.DescriptionReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.NoReplacementPruner;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static br.ufsc.lapesd.freqel.federation.inject.dagger.modules.ModuleHelper.createEndpoint;
import static java.util.Objects.requireNonNull;

@Module
public abstract class ReasoningModule {
    @Binds public abstract ReplacementGenerator replacementGenerator(SubTermReplacementGenerator g);

    @Provides public static TBoxMaterializer
    tBoxMaterializer(FreqelConfig config, @Named("override") @Nullable TBoxMaterializer override) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(MATERIALIZER, String.class));
        return ModuleHelper.get(TBoxMaterializer.class, name);
    }

    @Provides @Singleton public static TBox tBox(@Named("override") @Nullable TBox override,
                                           FreqelConfig config,
                                           TBoxMaterializer materializer,
                                           SourceLoaderRegistry loaderReg) {
        if (override != null)
            return override;
        File refDir = new File("");
        TPEndpoint ep = null;
        String hdtPath = config.get(TBOX_HDT, String.class);
        if (hdtPath != null)
            ep = createEndpoint("hdt", hdtPath, loaderReg, refDir);
        if (ep == null) {
            String rdfPath = config.get(TBOX_RDF, String.class);
            if (rdfPath != null)
                ep = createEndpoint("rdf-file", rdfPath, loaderReg, refDir);
        }
        if (ep != null)
            return TPEndpointTBox.create(ep);
        TBoxSpec spec = config.get(MATERIALIZER_SPEC, TBoxSpec.class);
        materializer.load(spec);
        return materializer;
    }

    @Provides @Singleton public static EndpointReasoner
    endpointReasoner(FreqelConfig config, TBox tBox,
                     HeuristicEndpointReasoner heuristic) {
        String name = requireNonNull(config.get(ENDPOINT_REASONER, String.class));
        EndpointReasoner r = ModuleHelper.get(EndpointReasoner.class, name, heuristic);
        r.setTBox(tBox);
        return r;
    }

    @Provides @Singleton public static ReplacementPruner
    pruner(FreqelConfig config, DescriptionReplacementPruner descriptionPruner,
           NoReplacementPruner noPruner) {
        return config.get(REPLACEMENT_PRUNE_BY_DESCRIPTION, Boolean.class)
                ? descriptionPruner : noPruner;
    }
}
