package br.ufsc.lapesd.freqel.federation.spec.source;

import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.freqel.util.BackoffStrategy;
import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.util.ExponentialBackoff;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;

public class SPARQLServiceLoader implements SourceLoader {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLServiceLoader.class);
    private static final Set<String> NAMES = Sets.newHashSet("sparql");
    private static final Set<String> DESCRIPTION_NAMES = Sets.newHashSet("select", "ask");

    private @Nullable SourceCache sourceCache;
    private @Nonnull BackoffStrategy backoffStrategy = ExponentialBackoff.neverRetry();

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override public void setTempDir(@Nonnull File ignored) { }

    @Override public void setSourceCache(@Nullable SourceCache sourceCache) {
        this.sourceCache = sourceCache;
    }

    @Override public void setIndexingBackoffStrategy(@Nonnull BackoffStrategy strategy) {
        this.backoffStrategy = strategy;
    }

    @Override
    public @Nonnull Set<TPEndpoint> load(@Nonnull DictTree spec,
                                         @Nonnull File reference) throws SourceLoadException {
        String loader = spec.getString("loader", "").trim().toLowerCase();
        if (!loader.equals("sparql"))
            throw new IllegalArgumentException(this+" does not support loader="+loader);
        String uri = getURI(spec);

        SPARQLClient ep = new SPARQLClient(uri);
        setupDescription(spec, sourceCache, ep);
        return singleton(ep);
    }

    private void setupDescription(@Nonnull DictTree spec, @Nullable SourceCache cacheDir,
                                  @Nonnull SPARQLClient ep) {
        String descriptionType = spec.getString("description", "ask");
        if (!DESCRIPTION_NAMES.contains(descriptionType)) {
            throw new IllegalArgumentException("Bad description key: "+descriptionType+
                    " expected one of"+DESCRIPTION_NAMES);
        }
        boolean fetchClasses = spec.getBoolean("fetchClasses", true);
        SelectDescription description = null;
        if (descriptionType.equalsIgnoreCase("select")) {
            if (cacheDir != null) {
                try {
                    Stopwatch sw = Stopwatch.createStarted();
                    description = SelectDescription.fromCache(ep, cacheDir, ep.getURI());
                    logger.debug("Loaded SelectDescription for {} from {} in {}ms",
                                 ep.getURI(), cacheDir.getDir(),
                                 sw.elapsed(TimeUnit.MICROSECONDS) / 1000.0);
                } catch (IOException e) {
                    logger.error("Failed to load SelectDescription from cache dir {}",
                            cacheDir.getDir(), e);
                }
                if (description == null) {
                    description = new SelectDescription(ep, fetchClasses);
                    description.saveWhenReady(cacheDir, ep.getURI());
                }
            } else {
                description = new SelectDescription(ep, fetchClasses);
            }
            ep.setDescription(description);
        }
        if (description != null)
            description.setBackoffStrategy(backoffStrategy);
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
