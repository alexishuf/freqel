package br.ufsc.lapesd.riefederator.federation.spec.source;

import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URI;
import java.util.Set;

import static java.util.Collections.singleton;

public class SPARQLServiceLoader implements SourceLoader {
    private static final Set<String> NAMES = Sets.newHashSet("sparql");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override
    public @Nonnull Set<Source> load(@Nonnull DictTree spec,
                                     @Nonnull File reference) throws SourceLoadException {
        String loader = spec.getString("loader", "").trim().toLowerCase();
        if (!loader.equals("sparql"))
            throw new IllegalArgumentException(this+" does not support loader="+loader);
        String uri = getURI(spec);
        boolean fetchClasses = spec.getBoolean("fetchClasses", true);
        ARQEndpoint ep = ARQEndpoint.forService(uri);
        return singleton(new Source(new SelectDescription(ep, fetchClasses), ep, uri));
    }

    private @Nonnull String getURI(@Nonnull DictTree spec) throws SourceLoadException {
        String uri = spec.getString("uri", null);
        if (uri == null)
            throw new SourceLoadException("Missing SPARQL service uri", spec);
        try {
            //noinspection ResultOfMethodCallIgnored
            URI.create(uri);
        } catch (IllegalArgumentException e) {
            throw new SourceLoadException("Bad SPARQL service URI: "+uri, e, spec);
        }
        return uri;
    }
}
