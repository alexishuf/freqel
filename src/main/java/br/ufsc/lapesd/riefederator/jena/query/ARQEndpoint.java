package br.ufsc.lapesd.riefederator.jena.query;

import br.ufsc.lapesd.riefederator.model.SPARQLString;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.IteratorResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.http.client.HttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.query.EstimatePolicy.*;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.jena.query.QueryExecutionFactory.create;
import static org.apache.jena.query.QueryExecutionFactory.sparqlService;

public class ARQEndpoint extends AbstractTPEndpoint implements CQEndpoint {
    @SuppressWarnings("Immutable")
    private final @Nonnull Function<String, QueryExecution> executionFactory;
    @SuppressWarnings("Immutable")
    private final @Nonnull Runnable closer;
    private final @Nullable String name;
    private final boolean local;

    protected ARQEndpoint(@Nullable String name,
                          @Nonnull Function<String, QueryExecution> executionFactory,
                          @Nonnull Runnable closer, boolean local) {
        this.executionFactory = executionFactory;
        this.name = name;
        this.closer = closer;
        this.local = local;
    }

    public @Nullable String getName() {
        return name;
    }

    public boolean isLocal() {
        return local;
    }

    public boolean isEmpty() {
        try (QueryExecution execution = executionFactory.apply("ASK WHERE {?s ?p ?o.}")) {
            return !execution.execAsk();
        }
    }

    /* ~~~ static method factories over some common source types  ~~~ */

    public static ARQEndpoint forModel(@Nonnull Model model) {
        String name = String.format("%s@%x", model.getClass().getSimpleName(),
                                             System.identityHashCode(model));
        return forModel(model, name);
    }
    public static ARQEndpoint forModel(@Nonnull Model model, @Nonnull String name) {
        return new ARQEndpoint(name, sparql -> create(sparql, model), () -> {}, true);
    }
    public static ARQEndpoint forDataset(@Nonnull Dataset ds) {
        return new ARQEndpoint(ds.toString(), sparql -> create(sparql, ds), () -> {}, true);
    }
    public static ARQEndpoint forCloseableDataset(@Nonnull Dataset ds) {
        return new ARQEndpoint(ds.toString(), sparql -> create(sparql, ds), ds::close, true);
    }

    public static ARQEndpoint forService(@Nonnull String uri) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql), () -> {}, false);
    }
    public static ARQEndpoint forService(@Nonnull String uri, @Nonnull HttpClient client) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql, client),
                               () -> {}, false);
    }
    public static ARQEndpoint forService(@Nonnull String uri, @Nonnull HttpClient client,
                                         @Nonnull HttpContext context) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql, client, context),
                               () -> {}, false);
    }

    /* ~~~ method overloads and implementations  ~~~ */

    @Override
    public String toString() {
        return name != null ? String.format("ARQEndpoint(%s)", name) : super.toString();
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        switch (capability) {
            case PROJECTION:
            case DISTINCT:
            case SPARQL_FILTER:
            case ASK:
                return true;
            default:
                return false;
        }
    }

    @Override
    public @Nonnull
    Results query(@Nonnull CQuery query) {
        ModifierUtils.check(this, query.getModifiers());
        PrefixDict dict = query.getPrefixDict(StdPrefixDict.STANDARD);
        SPARQLString sparql = new SPARQLString(query, dict, query.getModifiers());
        if (sparql.getType() == SPARQLString.Type.ASK) {
            try (QueryExecution exec = executionFactory.apply(sparql.getString())) {
                if (exec.execAsk())
                    return new CollectionResults(singleton(MapSolution.EMPTY), emptySet());
                return new CollectionResults(emptySet(), emptySet());
            }
        } else {
            QueryExecution exec = executionFactory.apply(sparql.getString());
            try {
                ResultSet rs = exec.execSelect();
                return new IteratorResults(new TransformIterator<>(rs, JenaSolution::new),
                                           sparql.getVarNames()) {
                    @Override
                    public void close() throws ResultsCloseException {
                        exec.close();
                    }
                };
            } catch (Throwable t) {
                exec.close();
                throw t;
            }
        }
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        if (query.isEmpty()) return Cardinality.EMPTY;
        if (isLocal() && isEmpty()) return Cardinality.EMPTY;
        if (policy == 0)
            return Cardinality.UNSUPPORTED;
        if (isLocal() && !canLocal(policy))
            return Cardinality.UNSUPPORTED;
        if (!isLocal() && !canRemote(policy))
            return Cardinality.UNSUPPORTED;

        PrefixDict dict = query.getPrefixDict(StdPrefixDict.EMPTY);
        ImmutableSet<Modifier> mods = query.getModifiers();
        if ((isLocal() && !canQueryLocal(policy)) || (!isLocal() && !canQueryRemote(policy)))
            mods = ImmutableSet.<Modifier>builder().addAll(mods).add(Ask.ADVISED).build();

        SPARQLString sparql = new SPARQLString(query, dict, mods);
        if (sparql.getType() == SPARQLString.Type.ASK) {
            try (QueryExecution exec = executionFactory.apply(sparql.getString())) {
                return exec.execAsk() ? Cardinality.exact(1) : Cardinality.EMPTY;
            }
        } else {
            String withLimit = sparql.getString() + "LIMIT "+ Math.max(limit(policy), 4) +"\n";
            try (QueryExecution exec = executionFactory.apply(withLimit)) {
                ResultSet results = exec.execSelect();
                int count = 0;
                boolean exhausted = false;
                Stopwatch sw = Stopwatch.createStarted();
                while (!exhausted && sw.elapsed(TimeUnit.MILLISECONDS) < 50) {
                    exhausted = !results.hasNext();
                    if (!exhausted) {
                        results.next();
                        ++count;
                    }
                }
                return count == 0 ? Cardinality.EMPTY
                        : (exhausted ? Cardinality.exact(count) : Cardinality.lowerBound(count));
            }
        }
//        return Cardinality.UNSUPPORTED;
    }

    @Override
    public void close() {
        closer.run();
    }
}
