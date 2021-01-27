package br.ufsc.lapesd.freqel.util;

import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.results.Results;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Function;

import static java.lang.String.format;
import static org.apache.jena.query.QueryExecutionFactory.create;
import static org.apache.jena.query.QueryExecutionFactory.sparqlService;

public class DelayedARQEndpoint extends ARQEndpoint {
    private final int delayMs;

    public DelayedARQEndpoint(int delayMs, @Nullable String name,
                              @Nonnull Function<Query, QueryExecution> executionFactory,
                              @Nullable Transactional transactional,
                              @Nonnull Runnable closer, boolean local) {
        super(name, executionFactory, transactional, closer, local);
        this.delayMs = delayMs;
    }

    public DelayedARQEndpoint(int delayMs, @Nonnull Model model) {
        this(delayMs, format("%s@%x", model.getClass().getSimpleName(),
                System.identityHashCode(model)), sparql -> create(sparql, model),
                null, () -> {}, true);
    }

    public DelayedARQEndpoint(int delayMs, @Nonnull String uri) {
        this(delayMs, uri, sparql -> sparqlService(uri, sparql), null,
             () -> {}, false);
    }

    @Override
    public @Nonnull Results doQuery(@Nonnull Query query, boolean isAsk,
                                    @Nonnull Set<String> vars) {
        try {
            if (delayMs >= 0) Thread.sleep(delayMs);
        } catch (InterruptedException ignored) { }
        return super.doQuery(query, isAsk, vars);
    }
}
