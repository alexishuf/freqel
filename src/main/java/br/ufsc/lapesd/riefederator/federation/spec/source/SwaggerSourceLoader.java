package br.ufsc.lapesd.riefederator.federation.spec.source;

import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.parser.APIDescriptionParseException;
import br.ufsc.lapesd.riefederator.webapis.parser.SwaggerParser;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class SwaggerSourceLoader implements SourceLoader {
    private static final Logger logger = LoggerFactory.getLogger(SwaggerSourceLoader.class);
    private static final @Nonnull Set<String> NAMES = Sets.newHashSet("swagger");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override
    public @Nonnull Set<Source> load(@Nonnull DictTree spec, @Nullable SourceCache ignored,
                                     @Nonnull File reference) throws SourceLoadException {
        String loaderKey = spec.getString("loader", "").trim().toLowerCase();
        checkArgument(loaderKey.equals("swagger"), this+"does not support loader="+loaderKey);

        SwaggerParser parser = getParser(spec, reference);
        Set<String> white = getStringSet(spec, "whitelist");
        Set<String> black = getStringSet(spec, "blacklist");
        int discarded = 0, failed = 0;
        Set<Source> sources = new HashSet<>();
        for (String endpoint : parser.getEndpoints()) {
            if ((!white.isEmpty() && !white.contains(endpoint)) || black.contains(endpoint)) {
                ++discarded;
                continue; //skip
            }
            try {
                WebAPICQEndpoint ep = parser.getEndpoint(endpoint);
                sources.add(ep.asSource());
            } catch (APIDescriptionParseException e) {
                if (spec.getBoolean("stop-on-error", false))
                    throw e;
                ++failed;
                logger.warn("Discarding endpoint {} due to SwaggerParser error: {}",
                            endpoint, e.getMessage());
            }
        }

        logger.info("Loaded {} endpoints (discarded {} and {} failed parsing) from {}",
                    sources.size(), discarded, failed,
                    spec.getString("file", spec.getString("uri", null)));
        return sources;
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
        String uri = spec.getString("uri", null);
        if (file != null) {
            try {
                File child = new File(file);
                File resolved = child.isAbsolute() ? child : new File(reference, child.getPath());
                return factory.fromFile(resolved);
            } catch (IOException e) {
                String message = "Could not load from file" + file + ": " + e.getMessage();
                throw new SourceLoadException(message, e, spec);
            }
        } else if (uri != null) {
            try {
                return factory.fromURL(uri);
            } catch (IOException e) {
                String message = "Could not load from URI" + uri + ": " + e.getMessage();
                throw new SourceLoadException(message, e, spec);
            }
        } else {
            throw new SourceLoadException("Neither file not uri properties are present", spec);
        }
    }
}
