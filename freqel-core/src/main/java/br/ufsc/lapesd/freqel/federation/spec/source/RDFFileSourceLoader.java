package br.ufsc.lapesd.freqel.federation.spec.source;

import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.BackoffStrategy;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.github.lapesd.rdfit.components.jena.JenaHelpers;
import com.github.lapesd.rdfit.errors.RDFItException;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;

public class RDFFileSourceLoader implements SourceLoader {
    private static final @Nonnull Pattern URI_RX = Pattern.compile("^[^:]+:");
    private static final @Nonnull Set<String> NAMES = Sets.newHashSet("rdf-file", "rdf-url");
    private static final @Nonnull Set<String> LOCATION_KEYS =
            Sets.newHashSet("location", "locations", "file", "files", "url", "urls", "uri", "uris");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override public void setTempDir(@Nonnull File ignored) { }

    @Override public void setSourceCache(@Nullable SourceCache ignored) { }

    @Override public void setIndexingBackoffStrategy(@Nonnull BackoffStrategy ignored) { }

    @Override
    public @Nonnull Set<TPEndpoint> load(@Nonnull DictTree spec,
                                         @Nonnull File reference) throws SourceLoadException {
        Model model = ModelFactory.createDefaultModel();
        List<String> nameParts = new ArrayList<>();
        String loader = spec.getString("loader", "");
        if (!loader.equals("rdf-file")) {
            throw new IllegalArgumentException("Loader " + loader + " not supported by " + this);
        } else if (LOCATION_KEYS.stream().noneMatch(spec::containsKey)) {

            throw new SourceLoadException("No "+String.join("/", LOCATION_KEYS)+
                                          " property present in this source spec!", spec);
        }
        for (String key : LOCATION_KEYS) {
            for (Object source : spec.getListNN(key))
                nameParts.add(loadSource(spec, source.toString(), model, reference));
        }
        String name = spec.getString("name", nameParts.toString());

        return singleton(ARQEndpoint.forModel(model, name));
    }

    private @Nonnull String loadSource(@Nonnull DictTree spec, @Nonnull String src,
                                       @Nonnull Model model,
                                       @Nonnull File ref) throws SourceLoadException {
        if (!URI_RX.matcher(src).find()) {
            src = new File(src).isAbsolute() ? src : new File(ref, src).getAbsolutePath();
        }
        try {
            JenaHelpers.toGraph(model.getGraph(), src);
        } catch (RDFItException e) {
            throw new SourceLoadException("Problem parsing source "+src, e, spec);
        } catch (RuntimeException|Error e) {
            throw new SourceLoadException("Unexpected exception parsing source "+src, e, spec);
        }
        return src;
    }
}
