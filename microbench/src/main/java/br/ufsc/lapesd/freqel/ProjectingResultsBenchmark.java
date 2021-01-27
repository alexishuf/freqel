package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.deprecated.MapProjectingResults;
import br.ufsc.lapesd.freqel.jena.query.JenaBindingResults;
import br.ufsc.lapesd.freqel.jena.query.JenaBindingSolution;
import br.ufsc.lapesd.freqel.jena.query.JenaSolution;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.query.results.impl.ProjectingResults;
import com.google.common.collect.ImmutableSet;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.XSD;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jena.rdf.model.ResourceFactory.*;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class ProjectingResultsBenchmark {
    private static final String EX = "http://example.org/ns#";
    private static final URI xsdInt = new StdURI(XSD.xint.getURI());
    private static final Property value = createProperty(EX+"value");
    private static final int ROWS = 128;

    private List<JenaSolution> jenaSolutions;
    private List<JenaBindingSolution> bindingSolutions;
    private List<MapSolution> mapSolutions;

    @Setup(Level.Trial)
    public void setUp() {
        mapSolutions = new ArrayList<>(ROWS);
        Model model = ModelFactory.createDefaultModel();
        for (int i = 0; i < ROWS; i++) {
            Resource uri = createResource(EX + i);
            Literal lit = createTypedLiteral(i);
            model.add(uri, value, lit);

            StdLit stdLit = StdLit.fromUnescaped(String.valueOf(i), xsdInt);
            mapSolutions.add(MapSolution.builder().put("x", new StdURI(EX+i))
                                                  .put("y", stdLit).build());
        }
        jenaSolutions = new ArrayList<>(ROWS);
        String queryStr = "PREFIX ex: <" + EX + ">\n" +
                "SELECT * WHERE { ?x ex:value ?y. }";
        try (QueryExecution exec = QueryExecutionFactory.create(queryStr, model)) {
            ResultSet resultSet = exec.execSelect();
            JenaSolution.Factory factory = new JenaSolution.Factory(resultSet.getResultVars());
            while (resultSet.hasNext())
                jenaSolutions.add(factory.transform(resultSet.next()));
        }

        bindingSolutions = new ArrayList<>(ROWS);
        try (QueryExecution exec = QueryExecutionFactory.create(queryStr, model)) {
            JenaBindingResults results = new JenaBindingResults(exec.execSelect(), null);
            results.forEachRemainingThenClose(s -> bindingSolutions.add((JenaBindingSolution)s));
        }
    }

    @Benchmark
    public List<Term> mapProjectJenaSolution() {
        List<Term> terms = new ArrayList<>(ROWS);
        CollectionResults results = new CollectionResults(jenaSolutions, ImmutableSet.of("x", "y"));
        MapProjectingResults projected = new MapProjectingResults(results, ImmutableSet.of("x"));
        projected.forEachRemainingThenClose(s -> terms.add(s.get("x")));
        return terms;
    }

    @Benchmark
    public List<Term> mapProjectBindingSolution() {
        List<Term> terms = new ArrayList<>(ROWS);
        CollectionResults results = new CollectionResults(bindingSolutions, ImmutableSet.of("x", "y"));
        MapProjectingResults projected = new MapProjectingResults(results, ImmutableSet.of("x"));
        projected.forEachRemainingThenClose(s -> terms.add(s.get("x")));
        return terms;
    }

    @Benchmark
    public List<Term> mapProjectMapSolution() {
        List<Term> terms = new ArrayList<>(ROWS);
        CollectionResults results = new CollectionResults(mapSolutions, ImmutableSet.of("x", "y"));
        MapProjectingResults projected = new MapProjectingResults(results, ImmutableSet.of("x"));
        projected.forEachRemainingThenClose(s -> terms.add(s.get("x")));
        return terms;
    }

    @Benchmark
    public List<Term> projectJenaSolution() {
        List<Term> terms = new ArrayList<>(ROWS);
        CollectionResults results = new CollectionResults(jenaSolutions, ImmutableSet.of("x", "y"));
        ProjectingResults projected = new ProjectingResults(results, ImmutableSet.of("x"));
        projected.forEachRemainingThenClose(s -> terms.add(s.get("x")));
        return terms;
    }

    @Benchmark
    public List<Term> projectBindingSolution() {
        List<Term> terms = new ArrayList<>(ROWS);
        CollectionResults results = new CollectionResults(bindingSolutions, ImmutableSet.of("x", "y"));
        ProjectingResults projected = new ProjectingResults(results, ImmutableSet.of("x"));
        projected.forEachRemainingThenClose(s -> terms.add(s.get("x")));
        return terms;
    }

    @Benchmark
    public List<Term> projectMapSolution() {
        List<Term> terms = new ArrayList<>(ROWS);
        CollectionResults results = new CollectionResults(mapSolutions, ImmutableSet.of("x", "y"));
        ProjectingResults projected = new ProjectingResults(results, ImmutableSet.of("x"));
        projected.forEachRemainingThenClose(s -> terms.add(s.get("x")));
        return terms;
    }
}
