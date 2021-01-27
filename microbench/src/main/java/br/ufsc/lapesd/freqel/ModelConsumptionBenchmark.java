package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.jena.query.JenaBindingResults;
import br.ufsc.lapesd.freqel.jena.query.JenaSolution;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.query.results.impl.IteratorResults;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.vocabulary.XSD;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.fromJena;
import static org.apache.jena.rdf.model.ResourceFactory.*;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class ModelConsumptionBenchmark {
    private static final String EX = "http://example.org/ns#";
    private static final URI xsdInt = new StdURI(XSD.xint.getURI());
    private static final Property value = createProperty(EX+"value");
    private static final int ROWS = 128;

    private Model model;
    private Query query;

    @Setup(Level.Trial)
    public void setUp() {
        model = ModelFactory.createDefaultModel();
        for (int i = 0; i < ROWS; i++)
            model.add(createResource(EX+i), value, createTypedLiteral(i));
        query = QueryFactory.create("PREFIX ex: <" + EX + ">\n" +
                                    "SELECT * WHERE { ?x ex:value ?y. }");
    }

    private @Nonnull Results createJenaSolution() {
        QueryExecution exec = QueryExecutionFactory.create(query, model);
        ResultSet rs = exec.execSelect();
        Set<String> vars = new HashSet<>(rs.getResultVars());
        JenaSolution.Factory solFac = new JenaSolution.Factory(vars);
        return new IteratorResults(new TransformIterator<>(rs, solFac), vars) {
            @Override
            public void close() throws ResultsCloseException {
                exec.close();
            }
        };
    }

    /**
     * Consume Jena Bindings objects and wrap them in JenaNodes
     */
    private @Nonnull Results createBindingsSolution() {
        QueryExecution exec = QueryExecutionFactory.create(query, model);
        ResultSet rs = exec.execSelect();
        return new JenaBindingResults(rs, exec);
    }

    /**
     * Consume Jena Bindings objects and eagerly convert the Nodes into StdTerms
     */
    private @Nonnull Results createUnpackedBindingsSolution() {
        QueryExecution exec = QueryExecutionFactory.create(query, model);
        ResultSet rs = exec.execSelect();
        IndexSet<String> vars = FullIndexSet.fromDistinct(rs.getResultVars());
        ArraySolution.ValueFactory factory = ArraySolution.forVars(vars);
        return new AbstractResults(vars) {

            @Override
            public int getReadyCount() {
                return hasNext() ? 1 : 0;
            }

            @Override
            public boolean hasNext() {
                return rs.hasNext();
            }

            @Override
            public @Nonnull Solution next() {
                Binding binding = rs.nextBinding();
                ArrayList<Term> values = new ArrayList<>();
                for (int i = 0; i < vars.size(); i++)
                    values.add(null);
                for (Iterator<Var> it = binding.vars(); it.hasNext(); ) {
                    Var var = it.next();
                    values.set(vars.indexOf(var.getName()), fromJena(binding.get(var)));
                }
                return factory.fromValues(values);
            }

            @Override
            public void close() throws ResultsCloseException {
                exec.close();
            }
        };
    }

    @Benchmark
    public Set<Solution> jenaSolutions() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        createJenaSolution().forEachRemainingThenClose(set::add);
        return set;
    }

    @Benchmark
    public Set<Solution> bindingsSolutions() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        createBindingsSolution().forEachRemainingThenClose(set::add);
        return set;
    }


    @Benchmark
    public Set<Solution> unpackedBindingsSolutions() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        createUnpackedBindingsSolution().forEachRemainingThenClose(set::add);
        return set;
    }
}
