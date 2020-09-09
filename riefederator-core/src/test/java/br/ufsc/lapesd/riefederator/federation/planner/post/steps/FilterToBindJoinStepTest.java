package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.planner.utils.FilterJoinPlannerTest;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.TPEndpointTest;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.streamPreOrder;
import static br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlannerTest.assertPlanAnswers;
import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class FilterToBindJoinStepTest implements TestContext {
    private static Map<String, TPEndpointTest.FusekiEndpoint> fusekiEndpoints
            = new ConcurrentHashMap<>();

    private static List<Supplier<FilterToBindJoinStep>> stepSuppliers =
            FilterJoinPlannerTest.suppliers.stream()
                    .map(s -> (Supplier<FilterToBindJoinStep>)
                            () -> new FilterToBindJoinStep(s.get()))
                    .collect(toList());

    private static @Nonnull TPEndpointTest.FusekiEndpoint getFusekiEndpoint(@Nonnull String file) {
        return fusekiEndpoints.computeIfAbsent(file, k -> {
            Model model = new TBoxSpec().addResource(FilterToBindJoinStepTest.class, file).loadModel();
            return new TPEndpointTest.FusekiEndpoint(DatasetFactory.create(model));
        });
    }

    private static @Nonnull Source createSPARQLClient(@Nonnull String file) {
        SPARQLClient ep = new SPARQLClient(getFusekiEndpoint(file).uri);
        return new Source(new SelectDescription(ep), ep);
    }

    private static @Nonnull Source createARQEndpoint(@Nonnull String file) {
        ARQEndpoint ep = ARQEndpoint.forService(getFusekiEndpoint(file).uri);
        return new Source(new SelectDescription(ep), ep);
    }

    private static @Nonnull Source createLocalARQEndpoint(@Nonnull String file) {
        Model model = new TBoxSpec().addResource(FilterToBindJoinStepTest.class, file).loadModel();
        ARQEndpoint ep = ARQEndpoint.forModel(model, file);
        return new Source(new SelectDescription(ep), ep);
    }

    private static final List<Function<String, Source>> sourceFactories = asList(
            FilterToBindJoinStepTest::createLocalARQEndpoint,
            FilterToBindJoinStepTest::createARQEndpoint,
            FilterToBindJoinStepTest::createSPARQLClient
    );

    private static @Nonnull EndpointQueryOp eq(Object... args) {
        return new EndpointQueryOp(new EmptyEndpoint(), createQuery(args));
    }

    @AfterClass
    public void afterClass() {
        for (TPEndpointTest.FusekiEndpoint ep : fusekiEndpoints.values())
            ep.close();
        fusekiEndpoints.clear();
    }

    @DataProvider
    public static Object[][] testQueryData() {
        String prolog = "PREFIX ex: <"+EX+">\n" +
                "PREFIX foaf: <"+ FOAF.NS +">\n" +
                "PREFIX xsd: <"+ XSD.NS +">\n";
        return Stream.of(
                asList(asList("str-join-left.ttl", "str-join-right.ttl"), prolog+
                        "SELECT ?x1 ?y1 ?z\n" +
                        "WHERE {\n" +
                        "  ?x1 foaf:title ?y1 .\n" +
                        "  ?x2 ex:mainTitle ?y2 ; ex:author/foaf:name ?z .\n" +
                        "  FILTER(str(?y1) = str(?y2))\n" +
                        "}",
                        asList(
                                MapSolution.builder().put(x1, new StdURI(EX+"book1"))
                                        .put(y1, fromUnescaped("Book 1", "en"))
                                        .put(z, fromUnescaped("Author 1", "en")).build(),
                                MapSolution.builder().put(x1, new StdURI(EX+"book2"))
                                        .put(y1, fromUnescaped("Book 2", "en"))
                                        .put(z, fromUnescaped("Author 2", "en")).build(),
                                MapSolution.builder().put(x1, new StdURI(EX+"book3"))
                                        .put(y1, fromUnescaped("Book 3", "en"))
                                        .put(z, fromUnescaped("Author 3", "en")).build()
                        )
                ),
                asList(asList("str-join-left.ttl", "str-join-right.ttl"), prolog+
                        "SELECT ?z WHERE {\n" +
                        "  ?x1 ex:genre \"Genre 2\"@en ; foaf:title          ?y1 .\n" +
                        "  ?x2 ex:mainTitle ?y2        ; ex:author/foaf:name ?z  .\n" +
                        "  FILTER(str(?y1) = str(?y2)).\n" +
                        "}",
                        asList(MapSolution.build(z, fromUnescaped("Author 2", "en"))))
        ).flatMap(base -> sourceFactories.stream().map(f -> {
            ArrayList<Object> copy = new ArrayList<>(base);
            @SuppressWarnings("unchecked") List<String> files = (List<String>)copy.remove(0);
            List<Source> sources = files.stream().map(f).collect(toList());
            copy.add(0, sources);
            return copy;
        })).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testQueryData")
    public void testDefaultQuery(@Nonnull List<Source> sources,
                                 @Nonnull String sparql,
                                 @Nonnull List<Solution> expected) throws SPARQLParseException {
        try (Federation federation = Federation.createDefault()) {
            sources.forEach(federation::addSource);
            Op query = SPARQLParser.strict().parse(sparql);
            List<Solution> actual = new ArrayList<>();
            federation.query(query).forEachRemainingThenClose(actual::add);
            assertEquals(new HashSet<Solution>(actual), new HashSet<Solution>(expected));
            assertEquals(actual.size(), expected.size());
        }
    }


    @Test(dataProvider = "testQueryData")
    public void testDefaultPlanNoProduct(@Nonnull List<Source> sources, @Nonnull String sparql,
                                         @Nonnull List<Solution> ignored) throws SPARQLParseException {
        try (Federation federation = Federation.createDefault()) {
            sources.forEach(federation::addSource);
            Op query = SPARQLParser.strict().parse(sparql);
            Op plan = federation.plan(query);
            assertPlanAnswers(plan, query);
            assertEquals(streamPreOrder(plan).filter(CartesianOp.class::isInstance).count(), 0);
        }
    }

    @DataProvider
    public static @Nonnull Object[][] testStepData() {
        String prolog = "PREFIX xsd: <"+XSD.NS+">\n" +
                "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                "PREFIX foaf: <"+ FOAF.NS +">\n" +
                "PREFIX ex: <"+EX+">\n";
        return Stream.of(
                // no work to be done
                asList(eq(Alice, knows, x), emptySet(),
                       prolog+"SELECT * WHERE {ex:Alice foaf:knows ?x}", null),
                // no work to be done
                asList(JoinOp.create(eq(x, knows, y), eq(y, age, u, SPARQLFilter.build("?u > 23"))),
                       emptySet(),
                       prolog+"SELECT * WHERE {?x foaf:knows ?y . ?y foaf:age ?u FILTER(?u > 23)}",
                       null),
                // push filter into join
                asList(JoinOp.builder(eq(x, age, u), eq(x, age, v))
                                .add(SPARQLFilter.build("?u > ?v")).build(),
                       emptySet(),
                       prolog+"SELECT * WHERE {?x foaf:age ?u ; foaf:age ?v FILTER(?u > ?v)}",
                       (Predicate<Op>)o -> {
                            if (!(o instanceof JoinOp)) return false;
                            if (!o.modifiers().filters().isEmpty()) return false;
                            return o.getChildren().stream().allMatch(EndpointQueryOp.class::isInstance);
                       }),
                // replace product with join
                asList(CartesianOp.builder()
                                .add(eq(Alice, age, u, SPARQLFilter.build("?u > 23")))
                                .add(eq(Alice, ageEx, v))
                                .add(SPARQLFilter.build("?u > ?v")).build(),
                       emptySet(), prolog+"SELECT * WHERE {\n" +
                                "  ex:Alice foaf:age ?u ; ex:age ?v\n" +
                                "    FILTER(?u > 23)\nFILTER(?u > ?v).\n" +
                                "}",
                       (Predicate<Op>)o -> {
                            if (!(o instanceof JoinOp)) return false;
                            return o.modifiers().filters().isEmpty();
                       })
        ).flatMap(l -> stepSuppliers.stream().map(s -> {
            ArrayList<Object> copy = new ArrayList<>(l);
            copy.add(0, s);
            return copy;
        })).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testStepData", groups = {"fast"})
    public void testStep(@Nonnull Supplier<FilterToBindJoinStep> supplier, @Nonnull Op plan,
                         @Nonnull Set<RefEquals<Op>> shared, @Nonnull String querySparql,
                         @Nullable Predicate<Op> checker) throws SPARQLParseException {
        Op query = SPARQLParser.strict().parse(querySparql);
        assertPlanAnswers(plan, query);
        Op actual = supplier.get().plan(TreeUtils.deepCopy(plan), shared);
        assertPlanAnswers(actual, query);
        if (checker != null)
            assertTrue(checker.test(actual));
    }
}