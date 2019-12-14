package br.ufsc.lapesd.riefederator.jena.query;

import br.ufsc.lapesd.riefederator.model.SPARQLString;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.impl.IteratorResults;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import com.google.errorprone.annotations.Immutable;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.http.client.HttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.jena.query.QueryExecutionFactory.create;
import static org.apache.jena.query.QueryExecutionFactory.sparqlService;

@ThreadSafe
@Immutable
public class ARQEndpoint implements CQEndpoint {
    @SuppressWarnings("Immutable")
    private final @Nonnull Function<String, QueryExecution> executionFactory;
    private final @Nullable String name;

    public ARQEndpoint(@Nonnull Function<String, QueryExecution> executionFactory) {
        this.executionFactory = executionFactory;
        this.name = null;
    }

    public ARQEndpoint(@Nonnull String name,
                       @Nonnull Function<String, QueryExecution> executionFactory) {
        this.executionFactory = executionFactory;
        this.name = name;
    }

    public @Nullable String getName() {
        return name;
    }

    /* ~~~ static method factories over some common source types  ~~~ */

    public static ARQEndpoint forModel(@Nonnull Model model) {
        return new ARQEndpoint(model.toString(), sparql -> create(sparql, model));
    }
    public static ARQEndpoint forDataset(@Nonnull Dataset dataset) {
        return new ARQEndpoint(dataset.toString(), sparql -> create(sparql, dataset));
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
    public @Nonnull Results query(@Nonnull CQuery query, @Nonnull PrefixDict dict) {
        SPARQLString sparql = new SPARQLString(query, dict);
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
}
