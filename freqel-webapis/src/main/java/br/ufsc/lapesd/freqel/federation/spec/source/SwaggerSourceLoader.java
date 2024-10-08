package br.ufsc.lapesd.freqel.federation.spec.source;

import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.BackoffStrategy;
import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.freqel.webapis.parser.APIDescriptionParseException;
import br.ufsc.lapesd.freqel.webapis.parser.SwaggerParser;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class SwaggerSourceLoader implements SourceLoader {
    private static final Logger logger = LoggerFactory.getLogger(SwaggerSourceLoader.class);
    private static final @Nonnull Set<String> NAMES = Sets.newHashSet("swagger");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override public void setTempDir(@Nonnull File ignored) { }

    @Override public void setSourceCache(@Nullable SourceCache sourceCache) { }

    @Override public void setIndexingBackoffStrategy(@Nonnull BackoffStrategy ignored) {}

    @Override
    public @Nonnull Set<TPEndpoint> load(@Nonnull DictTree spec,
                                         @Nonnull File reference) throws SourceLoadException {
        String loaderKey = spec.getString("loader", "").trim().toLowerCase();
        if (!loaderKey.equals("swagger"))
            throw new IllegalArgumentException(this+"does not support loader="+loaderKey);

        SwaggerParser parser = getParser(spec, reference);
        Set<String> white = getStringSet(spec, "whitelist");
        Set<String> black = getStringSet(spec, "blacklist");
        int discarded = 0, failed = 0;
        Map<String, TPEndpoint> sources = new HashMap<>();

        for (String endpoint : parser.getEndpoints()) {
            if ((!white.isEmpty() && !white.contains(endpoint)) || black.contains(endpoint)) {
                ++discarded;
                continue; //skip
            }
            try {
                WebAPICQEndpoint ep = parser.getEndpoint(endpoint);
                sources.put(endpoint, ep);
            } catch (APIDescriptionParseException e) {
                if (spec.getBoolean("stop-on-error", false))
                    throw e;
                ++failed;
                logger.warn("Discarding endpoint {} due to SwaggerParser error: {}",
                            endpoint, e.getMessage());
            }
        }
        for (ImmutablePair<String, String> pair : parser.getEquivalences()) {
            TPEndpoint l = sources.get(pair.left), r = sources.get(pair.right);
            if (l != null && r != null) {
                l.addAlternative(r);
                r.addAlternative(l);
            }
        }

        logger.info("Loaded {} endpoints (discarded {} and {} failed parsing) from {}",
                    sources.size(), discarded, failed,
                    spec.getString("file", spec.getString("url", null)));
        return new HashSet<>(sources.values());
    }

    private @Nonnull Set<String> getStringSet(DictTree spec, String name) {
        List<Object> list = spec.getListNN(name);
        HashSet<String> set = new HashSet<>(list.size());
        for (Object o : list) {
            if (o instanceof DictTree || o instanceof List)
                continue;
            if (o != null)
                set.add(o.toString());
        }
        return set;
    }

    private @Nonnull SwaggerParser getParser(@Nonnull DictTree spec,
                                             @Nonnull File reference) throws SourceLoadException {
        SwaggerParser.Factory factory = SwaggerParser.getFactory();
        String file = spec.getString("file", null);
        String url = spec.getString("url", null);
        if (file != null) {
            try {
                File child = new File(file);
                File resolved = child.isAbsolute() ? child : new File(reference, child.getPath());
                return factory.fromFile(resolved);
            } catch (IOException e) {
                String message = "Could not load from file " + file + ": " + e.getMessage();
                throw new SourceLoadException(message, e, spec);
            }
        } else if (url != null) {
            try {
                return factory.fromURL(url);
            } catch (IOException e) {
                String message = "Could not load from URL" + url + ": " + e.getMessage();
                throw new SourceLoadException(message, e, spec);
            }
        } else {
            throw new SourceLoadException("Neither file nor url properties are present", spec);
        }
    }
}
