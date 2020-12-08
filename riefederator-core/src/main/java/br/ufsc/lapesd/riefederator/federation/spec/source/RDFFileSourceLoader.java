package br.ufsc.lapesd.riefederator.federation.spec.source;

import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.jena.ModelUtils;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.util.parse.SourceIterationException;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singleton;

public class RDFFileSourceLoader implements SourceLoader {
    private static final @Nonnull Set<String> NAMES = Sets.newHashSet("rdf-file", "rdf-url");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override
    public @Nonnull Set<Source> load(@Nonnull DictTree spec, @Nullable SourceCache ignored,
                                     @Nonnull File reference) throws SourceLoadException {
        Model model = ModelFactory.createDefaultModel();
        List<String> nameParts = new ArrayList<>();
        String loader = spec.getString("loader", "");
        if (!loader.equals("rdf-file")) {
            throw new IllegalArgumentException("Loader "+loader+" not supported by "+this);
        } else if (!spec.containsKey("file") && !spec.containsKey("files")
                && !spec.containsKey("url" ) && !spec.containsKey("urls" )) {
            throw new SourceLoadException("No file/files/url/urls property present " +
                                          "in this source spec!", spec);
        }

        for (Object file : spec.getListNN("file"))
            nameParts.add(loadFile(spec, file.toString(), model, reference));
        for (Object file : spec.getListNN("files"))
            nameParts.add(loadFile(spec, file.toString(), model, reference));
        for (Object url : spec.getListNN("url"))
            nameParts.add(loadUrl(spec, url.toString(), model));
        for (Object url : spec.getListNN("urls"))
            nameParts.add(loadUrl(spec, url.toString(), model));
        String name = spec.getString("name", nameParts.toString());

        ARQEndpoint ep = ARQEndpoint.forModel(model, name);
        return singleton(new Source(new SelectDescription(ep, true), ep, name));
    }

    private @Nonnull String loadFile(@Nonnull DictTree spec, @Nonnull String path,
                                     @Nonnull Model model,
                                     @Nonnull File reference) throws SourceLoadException {
        File child = new File(path);
        File file = child.isAbsolute() ? child : new File(reference, child.getPath());
        try {
            ModelUtils.parseInto(model, file);
        } catch (SourceIterationException e) {
            throw new SourceLoadException("Problem parsing file "+file, e, spec);
        } catch (RuntimeException e) {
            throw new SourceLoadException("Unexpected exception parsing file "+file, e, spec);
        }
        return file.getAbsolutePath();
    }

    private @Nonnull String loadUrl(@Nonnull DictTree spec, @Nonnull String url,
                                    @Nonnull Model model) throws SourceLoadException {
        try {
            ModelUtils.parseInto(model, url);
            return url;
        } catch (SourceIterationException e) {
            throw new SourceLoadException("Problem loading URL "+url, e, spec);
        } catch (RuntimeException e) {
            throw new SourceLoadException("Unexpected exception loading URL "+url, e, spec);
        }
    }
}
