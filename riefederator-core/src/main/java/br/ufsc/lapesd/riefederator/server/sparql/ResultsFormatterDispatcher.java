package br.ufsc.lapesd.riefederator.server.sparql;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.server.sparql.impl.CSVResultsFormatter;
import br.ufsc.lapesd.riefederator.server.sparql.impl.JsonResultsFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class ResultsFormatterDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(ResultsFormatterDispatcher.class);
    private static final @Nonnull ResultsFormatterDispatcher INSTANCE;

    private LinkedHashMap<MediaType, ResultsFormatter> formatterMap = new LinkedHashMap<>();

    static {
        ResultsFormatterDispatcher d = new ResultsFormatterDispatcher();
        d.register(new JsonResultsFormatter());
        d.register(new CSVResultsFormatter());
        INSTANCE = d;
    }

    public static @Nonnull ResultsFormatterDispatcher getDefault() {
        return INSTANCE;
    }

    public @Nonnull ResultsFormatterDispatcher register(@Nonnull ResultsFormatter formatter) {
        for (MediaType type : formatter.outputMediaTypes()) {
            ResultsFormatter old = formatterMap.get(type);
            if (old != null && !old.equals(formatter)) {
                logger.info("Replacing formatter {} with {} for media type {}",
                            old, formatter, type);
            }
            formatterMap.put(type, formatter);
        }
        return this;
    }

    public @Nonnull FormattedResults format(@Nonnull Results results, boolean isAsk,
                                            @Nullable HttpHeaders headers,
                                            @Nullable UriInfo uriInfo) {
        checkState(!formatterMap.isEmpty(), "Disatcher cannot work on a empty formatterMap");
        if (uriInfo != null) {
            /* This is non-standard, but qonsole does this (and does not set the Accept header) */
            MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
            List<String> list = params.get("output");
            if (list == null || list.isEmpty()) list = params.get("out");
            list = list == null ? Collections.emptyList() : list;
            for (String s : list) {
                String string = s.trim().toLowerCase();
                try {
                    MediaType mediaType = MediaType.valueOf(string);
                    MediaType match = formatterMap.keySet().stream()
                            .filter(mediaType::isCompatible).findFirst().orElse(null);
                    if (match != null)
                        return formatterMap.get(match).format(results, isAsk, match);
                } catch (IllegalArgumentException ignored) { }
                MediaType match = formatterMap.keySet().stream()
                        .filter(m -> m.getSubtype().equals(string)).findFirst().orElse(null);
                if (match != null)
                    return formatterMap.get(match).format(results, isAsk, match);
            }
        }
        if (headers != null) {
            for (MediaType type : headers.getAcceptableMediaTypes()) {
                MediaType match = formatterMap.keySet().stream().filter(type::isCompatible)
                                                       .findFirst().orElse(null);
                if (match != null)
                    return formatterMap.get(match).format(results, isAsk, type);
            }
        }
        Map.Entry<MediaType, ResultsFormatter> def = formatterMap.entrySet().iterator().next();
        return def.getValue().format(results, isAsk, def.getKey());
    }
}
