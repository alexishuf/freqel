package br.ufsc.lapesd.riefederator.federation.spec.source;

import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

import static java.util.Collections.singleton;

public class SPARQLServiceLoader implements SourceLoader {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLServiceLoader.class);
    private static final Set<String> NAMES = Sets.newHashSet("sparql");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override
    public @Nonnull Set<Source> load(@Nonnull DictTree spec, @Nullable SourceCache cacheDir,
                                     @Nonnull File reference) throws SourceLoadException {
        String loader = spec.getString("loader", "").trim().toLowerCase();
        if (!loader.equals("sparql"))
            throw new IllegalArgumentException(this+" does not support loader="+loader);
        String uri = getURI(spec);
        boolean fetchClasses = spec.getBoolean("fetchClasses", true);
        ARQEndpoint ep = ARQEndpoint.forService(uri);

        SelectDescription description = null;
        if (cacheDir != null) {
            try {
                description = SelectDescription.fromCache(ep, cacheDir, uri);
            } catch (IOException e) {
                logger.error("Failed to load SelectDescription from cache dir {}",
                             cacheDir.getDir(), e);
            }
            if (description == null) {
                description = new SelectDescription(ep, fetchClasses);
                description.saveWhenReady(cacheDir, uri);
            }
        } else {
            description = new SelectDescription(ep, fetchClasses);
        }
        return singleton(new Source(description, ep, uri));
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
