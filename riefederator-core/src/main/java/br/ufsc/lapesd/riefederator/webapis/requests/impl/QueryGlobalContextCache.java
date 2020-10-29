package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.annotations.GlobalContextAnnotation;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class QueryGlobalContextCache {
    public static final @Nonnull String KEY = QueryGlobalContextCache.class.getSimpleName();
    private final @Nonnull Map<String, Response> cache = new HashMap<>();

    public static @Nonnull QueryGlobalContextCache get(@Nonnull CQuery query) {
        GlobalContextAnnotation ann = query.getQueryAnnotation(GlobalContextAnnotation.class);
        if (ann == null) {
            if (query instanceof MutableCQuery)
                ((MutableCQuery)query).annotate(ann = new GlobalContextAnnotation());
            else
                return new QueryGlobalContextCache();
        }
        return ann.computeIfAbsent(KEY, k -> new QueryGlobalContextCache());
    }

    public synchronized @Nonnull Response get(@Nonnull String uri, Supplier<Response> invoker) {
        Response cached = cache.get(uri);
        if (cached != null)
            return cached;
        Response response = invoker.get();
        response.bufferEntity();
        cache.put(uri, response);
        return response;
    }

}
