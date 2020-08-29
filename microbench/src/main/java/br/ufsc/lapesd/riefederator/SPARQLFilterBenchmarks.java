package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.jena.query.JenaBindingResults;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.XSD;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.util.Objects.requireNonNull;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDinteger;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDstring;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class SPARQLFilterBenchmarks {
    private static final String TEST_ROOT = "riefederator-core/src/test/resources/br/ufsc/lapesd" +
                                            "/riefederator/LargeRDFBench-reassembled/";
    private static final String ALPHABET = "abcdefghijklmnoprstuvxywz";
    private static final URI xsdStringURI = new StdURI(XSD.xstring.getURI());
    private static final URI xsdIntegerURI = new StdURI(XSD.integer.getURI());

    @Param({"std", "jena_graph", "jena_model"})
    private String termType;

    private CollectionResults inputSmallB5;
    private CollectionResults inputLargeB5;
    private List<SPARQLFilter> b5Filters;

    private @Nonnull Term addInteger(@Nonnull Term term, int off) {
        String lexical = String.valueOf(Integer.parseInt(term.asLiteral().getLexicalForm()) + off);
        switch (termType) {
            case "std":
                return StdLit.fromUnescaped(lexical, xsdIntegerURI);
            case "jena_graph":
                return JenaWrappers.fromJena(createLiteral(lexical, XSDinteger));
            case "jena_model":
                return JenaWrappers.fromJena(createTypedLiteral(lexical, XSDinteger));
        }
        throw new IllegalArgumentException("Bad termType="+termType);
    }

    private @Nonnull Term generateString(@Nonnull Random random) {
        StringBuilder b = new StringBuilder(20);
        for (int i = 0; i < 20; i++)
            b.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        switch (termType) {
            case "std":
                return StdLit.fromUnescaped(b.toString(), xsdStringURI);
            case "jena_graph":
                return JenaWrappers.fromJena(createLiteral(b.toString(), XSDstring));
            case "jena_model":
                return JenaWrappers.fromJena(createTypedLiteral(b.toString(), XSDstring));
        }
        throw new IllegalArgumentException("Bad termType="+termType);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        File dataDir = new File(TEST_ROOT + "/data");
        File[] files = dataDir.listFiles();
        if (files == null)
            throw new FileNotFoundException(dataDir.getAbsolutePath());
        Model data = ModelFactory.createDefaultModel();
        for (File file : files) {
            try (FileInputStream in = new FileInputStream(file)) {
                RDFDataMgr.read(data, in, Lang.TURTLE);
            }
        }
        File b5File = new File(TEST_ROOT + "/queries/B5");
        String b5 = IOUtils.toString(b5File.toURI(), StandardCharsets.UTF_8);
        Op parsedB5 = SPARQLParser.tolerant().parse(b5);
        b5Filters = new ArrayList<>(parsedB5.modifiers().filters());
        if (b5Filters.isEmpty())
            throw new IllegalArgumentException("Bad B5 query?");

        b5 = b5.replaceAll("SELECT\\s*[^}]*(WHERE|$)", "SELECT * $1")
               .replaceAll("FILTER\\s*\\(.*\\)\\s*\\.?\\s*(\n|$)", "");
        if (b5.contains("FILTER"))
            throw new RuntimeException("FILTER not removed from B5");
        try (QueryExecution exec = QueryExecutionFactory.create(b5, data)) {
            ResultSet rs = exec.execSelect();
            ArraySolution.ValueFactory fac = ArraySolution.forVars(rs.getResultVars());
            List<Solution> list = new ArrayList<>();
            new JenaBindingResults(rs, exec)
                    .forEachRemainingThenClose(s -> list.add(fac.fromSolution(s)));
            inputSmallB5 = new CollectionResults(list, fac.getVarNames());
        }
        inputLargeB5 = enlargeB5Results();
    }

    private @Nonnull CollectionResults enlargeB5Results() {
        List<Solution> list = new ArrayList<>();
        Random random = new Random(788543);
        ArraySolution.ValueFactory fac = ArraySolution.forVars(inputSmallB5.getVarNames());
        for (Solution base : inputSmallB5.getCollection()) {
            Term shared = generateString(random);
            ArraySolution solution = fac.fromSolution(base);
            solution.set("chromosome", shared);
            solution.set("lookupChromosome", shared);
            list.add(solution);
            for (int i = 0; i < 6; i++) {
                ArraySolution notSolution = fac.fromSolution(solution);
                Term stop = notSolution.get("stop");
                notSolution.set("position", addInteger(requireNonNull(stop), 1));
                list.add(notSolution);
            }
            for (int i = 0; i < 6; i++) {
                ArraySolution notSolution = fac.fromSolution(solution);
                Term start = notSolution.get("start");
                notSolution.set("position", addInteger(requireNonNull(start), -1));
                list.add(notSolution);
            }
            for (int i = 0; i < 12; i++) {
                ArraySolution notSolution = fac.fromSolution(base);
                notSolution.set("chromosome", generateString(random));
                notSolution.set("lookupChromosome", generateString(random));
                list.add(notSolution);
            }
        }
        return new CollectionResults(list, inputSmallB5.getVarNames());
    }

    @Benchmark
    public @Nonnull List<Solution> smallB5() {
        inputSmallB5.reset();
        List<Solution> list = new ArrayList<>();
        SPARQLFilterResults.applyIf(inputSmallB5, b5Filters).forEachRemainingThenClose(list::add);
        return list;
    }

    @Benchmark
    public @Nonnull List<Solution> largeB5() {
        inputLargeB5.reset();
        List<Solution> list = new ArrayList<>();
        SPARQLFilterResults.applyIf(inputLargeB5, b5Filters).forEachRemainingThenClose(list::add);
        return list;
    }
}
