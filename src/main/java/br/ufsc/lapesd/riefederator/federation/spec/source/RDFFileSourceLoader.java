package br.ufsc.lapesd.riefederator.federation.spec.source;

import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

public class RDFFileSourceLoader implements SourceLoader {
    private static final @Nonnull Set<String> NAMES = Sets.newHashSet("rdf-file", "rdf-url");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override
    public @Nonnull Set<Source> load(@Nonnull DictTree sourceSpec,
                                     @Nonnull File reference) throws SourceLoadException {
        String syntax = sourceSpec.getString("syntax", "").trim().toLowerCase();
        if (syntax.trim().isEmpty())
            syntax = null;
        Lang lang = RDFLanguages.fileExtToLang(syntax);
        if (lang == null)
            lang = RDFLanguages.nameToLang(syntax);
        if (lang == null)
            lang = RDFLanguages.shortnameToLang(syntax);
        if (lang == null)
            lang = RDFLanguages.contentTypeToLang(syntax);

        Model model = ModelFactory.createDefaultModel();
        String name;
        String loader = sourceSpec.getString("loader", "");
        if (loader.equals("rdf-file")) {
            name = loadFile(sourceSpec, lang, model, reference);
        } else if (loader.equals("rdf-url")) {
            name = loadUrl(sourceSpec, lang, model);
        } else {
            throw new IllegalArgumentException("Loader "+loader+" not supported by "+this);
        }

        ARQEndpoint ep = ARQEndpoint.forModel(model, name);
        return singleton(new Source(new SelectDescription(ep, true), ep, name));
    }

    private @Nonnull String loadFile(@Nonnull DictTree spec, @Nullable Lang lang,
                                     @Nonnull Model model,
                                     @Nonnull File reference) throws SourceLoadException {
        File child = new File(spec.getString("file", ""));
        File file = child.isAbsolute() ? child : new File(reference, child.getPath());
        RiotException firstException = null;
        Lang firstLang = null;
        for (Lang l : asList(lang, RDFLanguages.filenameToLang(file.getName()))) {
            if (l == null) continue;
            try (FileInputStream in = new FileInputStream(file)) {
                RDFDataMgr.read(model, in, l);
                return file.getAbsolutePath();
            } catch (RiotException e) {
                firstException = e;
                firstLang = l;
            } catch (IOException e) {
                String m = "Failed to parse file" + file + " due to IOException: " + e.getMessage();
                throw new SourceLoadException(m, e, spec);
            }
        }
        assert firstException != null;
        throw new SourceLoadException(firstLang+"Syntax error on file "+file+": "+
                                      firstException.getMessage(), firstException, spec);
    }

    private @Nonnull String loadUrl(@Nonnull DictTree spec, @Nullable Lang lang,
                                   @Nonnull Model model) throws SourceLoadException {
        String uri = spec.getString("uri", "");
        try {
            RDFDataMgr.read(model, uri, lang);
            return uri;
        } catch (RiotException e) {
            throw new SourceLoadException("Syntax error on URI "+uri+": "+ e.getMessage(), e, spec);
        } catch (RuntimeException e) {
            String m = "Failed to parse URI" + uri + " due to Exception: " + e.getMessage();
            throw new SourceLoadException(m, e, spec);
        }
    }
}
