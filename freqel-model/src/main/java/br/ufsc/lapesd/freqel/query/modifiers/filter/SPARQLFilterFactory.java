package br.ufsc.lapesd.freqel.query.modifiers.filter;

import br.ufsc.lapesd.freqel.query.modifiers.FilterParsingException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ServiceLoader;

public class SPARQLFilterFactory {
    private static @Nullable SPARQLFilterFactoryService service;

    public static @Nonnull SPARQLFilterFactoryService getService() {
        if (service != null)
            return service;
        Class<SPARQLFilterFactoryService> cls = SPARQLFilterFactoryService.class;
        SPARQLFilterFactoryService first = null;
        for (SPARQLFilterFactoryService svc : ServiceLoader.load(cls)) {
            if (first == null || svc.toString().compareTo(first.toString()) < 0)
                first = svc;
        }
        if (first == null) {
            throw new RuntimeException("No SPARQLFilterFactoryService provider found. " +
                                       "Consider adding freqel-jena to the classpath");
        }
        service = first;
        return first;
    }

    public static @Nonnull SPARQLFilter
    parseFilter(@Nonnull String filter) throws FilterParsingException {
        return getService().parse(filter);
    }

    public static @Nonnull SPARQLFilter
    wrapFilter(@Nonnull SPARQLFilterNode root) throws FilterParsingException {
        return getService().parse(root);
    }

    public static @Nonnull SPARQLFilterExecutor createExecutor() {
        return getService().createExecutor();
    }
}
