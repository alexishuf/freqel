package br.ufsc.lapesd.riefederator.jena.query;

import br.ufsc.lapesd.riefederator.model.SPARQLString;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.query.*;
import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.impl.IteratorResults;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.http.client.HttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

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

    public ARQEndpoint(@Nonnull Function<String, QueryExecution> executionFactory) {
        this(executionFactory, () -> {});
    }

    public ARQEndpoint(@Nonnull Function<String, QueryExecution> executionFactory,
                       @Nonnull Runnable closer) {
        this.executionFactory = executionFactory;
        this.name = null;
        this.closer = closer;
    }

    public ARQEndpoint(@Nonnull String name,
                       @Nonnull Function<String, QueryExecution> executionFactory) {
        this(name, executionFactory, () -> {});
    }

    public ARQEndpoint(@Nonnull String name,
                       @Nonnull Function<String, QueryExecution> executionFactory,
                       @Nonnull Runnable closer) {
        this.executionFactory = executionFactory;
        this.name = name;
        this.closer = closer;
    }

    public @Nullable String getName() {
        return name;
    }

    /* ~~~ static method factories over some common source types  ~~~ */

    public static ARQEndpoint forModel(@Nonnull Model model) {
        String name = String.format("%s@%x", model.getClass().getSimpleName(),
                                             System.identityHashCode(model));
        return new ARQEndpoint(name, sparql -> create(sparql, model));
    }
    public static ARQEndpoint forDataset(@Nonnull Dataset ds) {
        return new ARQEndpoint(ds.toString(), sparql -> create(sparql, ds));
    }
    public static ARQEndpoint forCloseableDataset(@Nonnull Dataset ds) {
        return new ARQEndpoint(ds.toString(), sparql -> create(sparql, ds), ds::close);
    }

    public static ARQEndpoint forService(@Nonnull String uri) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql));
    }
    public static ARQEndpoint forService(@Nonnull String uri, @Nonnull HttpClient client) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql, client));
    }
    public static ARQEndpoint forService(@Nonnull String uri, @Nonnull HttpClient client,
                                         @Nonnull HttpContext context) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql, client, context));
    }

    /* ~~~ method overloads and implementations  ~~~ */

    @Override
    public String toString() {
        return name != null ? String.format("ARQEndpoint(%s)", name) : super.toString();
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        switch (capability) {
            case PROJECTION:
            case DISTINCT:
            case ASK:
                return true;
            default:
                return false;
        }
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        ModifierUtils.check(this, query.getModifiers());
        PrefixDict dict = query.getPrefixDict(StdPrefixDict.EMPTY);
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
    public void close() {
        closer.run();
    }
}
