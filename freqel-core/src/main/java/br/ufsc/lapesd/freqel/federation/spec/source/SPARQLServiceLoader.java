package br.ufsc.lapesd.freqel.federation.spec.source;

import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.CompliantTSVSPARQLClient;
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
        CQEndpoint ep;
        if ("compliantTSV".equals(spec.getString("client")))
            ep = new CompliantTSVSPARQLClient(uri);
        else
            ep = new SPARQLClient(uri);
        ((AbstractTPEndpoint)ep).setDescription(setupDescription(spec, sourceCache, ep, uri));
        return singleton(ep);
    }

    private @Nonnull Description setupDescription(@Nonnull DictTree spec, @Nullable SourceCache cacheDir,
                                                  @Nonnull CQEndpoint ep, @Nonnull String uri) {
        String descriptionType = spec.getString("description", "ask");
        boolean fetchClasses = spec.getBoolean("fetchClasses", true);
        Description description = null;
        SelectDescription selectDescription = null;
        if (descriptionType.equalsIgnoreCase("ask")) {
            description = new AskDescription(ep);
        } else if (descriptionType.equalsIgnoreCase("select")) {
            if (cacheDir != null) {
                try {
                    Stopwatch sw = Stopwatch.createStarted();
                    description = selectDescription = SelectDescription.fromCache(ep, cacheDir, uri);
                    logger.debug("Loaded SelectDescription for {} from {} in {}ms",
                                 uri, cacheDir.getDir(),
                                 sw.elapsed(TimeUnit.MICROSECONDS) / 1000.0);
                } catch (IOException e) {
                    logger.error("Failed to load SelectDescription from cache dir {}",
                            cacheDir.getDir(), e);
                }
                if (description == null) {
                    description = selectDescription = new SelectDescription(ep, fetchClasses);
                    selectDescription.saveWhenReady(cacheDir, uri);
                }
            } else {
                description = selectDescription = new SelectDescription(ep, fetchClasses);
            }
        } else {
            throw new IllegalArgumentException("Bad description key: "+descriptionType+
                                               " expected select or ask");
        }
        if (selectDescription != null)
            selectDescription.setBackoffStrategy(backoffStrategy);
        return description;
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
