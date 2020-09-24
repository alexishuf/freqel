package br.ufsc.lapesd.riefederator.query.parse;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createTolerantQuery;
import static br.ufsc.lapesd.riefederator.query.parse.SPARQLParser.hidden;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class SPARQLParserTest implements TestContext {

    @DataProvider
    public static @Nonnull Object[][] parseData() {
        String prolog = "PREFIX xsd: <" + XSD.getURI() + ">\n" +
                        "PREFIX rdf: <"+ RDF.getURI() +">\n" +
                        "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                        "PREFIX foaf: <"+ FOAF.getURI() +">\n" +
                        "PREFIX ex: <"+ EX +">\n";
        return Stream.of(
                asList("", true, null, SPARQLParseException.class),
                asList("xxx", true, null, SPARQLParseException.class),
                asList("SELECT ?x WHERE {?x ex:p foaf:Agent.}", true, null, SPARQLParseException.class),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.}", true,
                       createQuery(x, p, o),
                       null),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT 10", true,
                       createQuery(x, p, o, Limit.of(10)),
                       null),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT -1", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT 0", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT ?x WHERE {?x ?p ?o.}", true,
                       createQuery(x, p, o, Projection.of("x")),
                       null),
                asList(prolog+"ASK WHERE {?x ?p ?o.}", true,
                       createQuery(x, p, o, Ask.INSTANCE),
                       null),
                asList(prolog+"ASK WHERE {?x rdf:type/rdfs:subClassOf* ?o.}", true,
                       null,
                       UnsupportedSPARQLFeatureException.class),
                asList("DESCRIBE <"+EX+"Alice>", true, null, UnsupportedSPARQLFeatureException.class),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(Alice, knows, x, x, knows, Bob),
                       null),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob),
                       null),
                asList(prolog+"SELECT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob, Projection.of("x")),
                       null),
                asList(prolog+"SELECT DISTINCT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob, Projection.of("x"), Distinct.INSTANCE),
                       null),
                asList(prolog+"SELECT * WHERE {\n" +
                              "?x foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                x, knows, hidden(0),
                                hidden(0), age, u,
                                SPARQLFilter.build("?u > 23"),
                                Projection.of("x", "u")),
                        null),
                asList(prolog+"SELECT * WHERE {\n" +
                              "?x foaf:name ?y; \n" +
                              "   foaf:knows/foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                x, name, y,
                                x, knows, hidden(0),
                                hidden(0), knows, hidden(1),
                                hidden(1), age, u,
                                SPARQLFilter.build("?u > 23"),
                                Projection.of("x", "y", "u")),
                        null),
                asList(prolog+"SELECT DISTINCT ?y WHERE {\n" +
                                "?x foaf:name ?y; \n" +
                                "   foaf:knows/foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                x, name, y,
                                x, knows, hidden(0),
                                hidden(0), knows, hidden(1),
                                hidden(1), age, u,
                                SPARQLFilter.build("?u > 23"),
                                Projection.of("y"), Distinct.INSTANCE),
                        null),
                asList(prolog+"SELECT * WHERE {\n" +
                                "?x foaf:name ?y ; \n" +
                                "   foaf:knows/foaf:knows/foaf:age ?u ;\n" +
                                "   foaf:isPrimaryTopicOf/foaf:title ?z .\n}", true,
                        createQuery(
                                x, name, y,
                                x, knows, hidden(0),
                                hidden(0), knows, hidden(1),
                                hidden(1), age, u,
                                x, isPrimaryTopicOf, hidden(2),
                                hidden(2), foafTitle, z,
                                Projection.of("x", "y", "u", "z")),
                        null),
                asList(prolog+"SELECT ?x ?z WHERE {\n?x foaf:name ?y}", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT ?x ?z WHERE {\n?x foaf:name ?y}", false,
                       createTolerantQuery(x, name, y, Projection.of("x", "z")), null),
                // simple union
                asList(prolog+"SELECT * WHERE {\n" +
                        "  { ?x foaf:knows ex:Alice . } UNION \n"+
                        "  { ?x foaf:knows ex:Bob. }\n" +
                        "}", true,
                        UnionOp.builder()
                                .add(new QueryOp(createQuery(x, knows, Alice)))
                                .add(new QueryOp(createQuery(x, knows, Bob)))
                                .build(), null),
                // filter placement in nodes
                asList(prolog+"SELECT * WHERE {\n" +
                                "  FILTER(REGEX(str(?x), \"^http:\")) \n" +
                                "  { ?x foaf:knows ex:Alice; \n" +
                                "       foaf:age ?y   FILTER(?y > 27) . } UNION \n"+
                                "  { ?x foaf:knows ex:Bob ; \n" +
                                "       foaf:age ?y . FILTER(?y > 23)   }\n" +
                                "  FILTER(?y < 40) .\n" +
                                "}", true,
                        UnionOp.builder()
                                .add(new QueryOp(
                                        createQuery(x, knows, Alice,
                                                    x, age,   y,
                                                    SPARQLFilter.build("?y > 27"))))
                                .add(new QueryOp(
                                        createQuery(x, knows, Bob,
                                                    x, age,   y,
                                                    SPARQLFilter.build("?y > 23")
                                        )))
                                .add(SPARQLFilter.build("REGEX(str(?x), \"^http:\")"))
                                .add(SPARQLFilter.build("?y < 40"))
                                .build(), null),
                // create a conjunction between a triple pattern and a UNION
                asList(prolog+"SELECT * WHERE {\n" +
                        "  ?x foaf:age ?u .\n" +
                        "  { ?x foaf:knows ex:Alice . } UNION { ?x foaf:knows ex:Bob }" +
                        "}", true,
                        ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(x, age, u)))
                                .add(UnionOp.builder()
                                        .add(new QueryOp(createQuery(x, knows, Alice)))
                                        .add(new QueryOp(createQuery(x, knows, Bob)))
                                        .build())
                                .build(), null),
                // create a conjunction between a union and a triple block with a path expression
                asList(prolog+"SELECT * WHERE {\n" +
                                "  { \n" +
                                "    {\n" +
                                "      ?x foaf:knows ex:Alice .\n" +
                                "      ?x foaf:age ?u . FILTER(?u < ?y)\n" +
                                "    } UNION \n" +
                                "    {?x foaf:knows ex:Bob \n" +
                                "     FILTER(REGEX(str(?x), \"^http\")) .} \n" +
                                "  } .\n" +
                                "  ?x foaf:knows/foaf:age ?y FILTER (?y > 23)." +
                                "}", true,
                        ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new QueryOp(
                                                createQuery(x, knows, Alice,
                                                            x, age,   u,
                                                            SPARQLFilter.build("?u < ?y"))))
                                        .add(new QueryOp(createQuery(
                                                x, knows, Bob,
                                                SPARQLFilter.build("REGEX(str(?x), \"^http\")"))))
                                        .build())
                                .add(new QueryOp(
                                        createQuery(x,         knows, spHidden0,
                                                    spHidden0,   age, y)))
                                .add(SPARQLFilter.build("?y > 23"))
                                .add(Projection.of("x", "u", "y"))
                                .build(), null),
                // parse an OPTIONAL clause
                asList(prolog+"SELECT * WHERE {\n" +
                                "?x foaf:knows ex:Alice . OPTIONAL {?x foaf:name ?y .} }", true,
                       ConjunctionOp.builder()
                               .add(new QueryOp(createQuery(x, knows, Alice)))
                               .add(new QueryOp(createQuery(x, name, y, Optional.EXPLICIT)))
                               .build(), null),
                // parse OPTIONAL with FILTER inside a UNION
                asList(prolog+"SELECT * WHERE {\n" +
                                " {\n" +
                                "    ?x foaf:knows ex:Alice .\n" +
                                "    OPTIONAL {\n" +
                                "      ?x foaf:name ?y .\n" +
                                "      ?x foaf:age ?u FILTER(?u > 23) .\n" +
                                "    }\n" +
                                "  } UNION {\n" +
                                "   ?x foaf:knows ex:Bob .\n" +
                                "  }\n" +
                                "}", true,
                        UnionOp.builder()
                                .add(ConjunctionOp.builder()
                                        .add(new QueryOp(createQuery(x, knows, Alice)))
                                        .add(new QueryOp(createQuery(
                                                x, name, y,
                                                x, age, u,
                                                SPARQLFilter.build("?u > 23"), Optional.EXPLICIT
                                        ))).build())
                                .add(new QueryOp(createQuery(x, knows, Bob)))
                                .build(), null),
                // OPTIONAL in conjunction with UNION
                asList(prolog+"SELECT ?x ?u WHERE {\n" +
                                "  {\n" +
                                "    {\n" +
                                "      ?x foaf:knows ex:Alice .\n" +
                                "    } UNION {\n" +
                                "      ?x foaf:knows ex:Bob .\n" +
                                "    }\n" +
                                "  }\n" +
                                "  OPTIONAL { ?x foaf:name ?y ; foaf:age ?u FILTER(?u < 23)}\n" +
                                "}", true,
                        ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new QueryOp(createQuery(x, knows, Alice)))
                                        .add(new QueryOp(createQuery(x, knows, Bob)))
                                        .build())
                                .add(new QueryOp(createQuery(
                                        x, name, y,
                                        x, age, u,
                                        SPARQLFilter.build("?u < 23"), Optional.EXPLICIT)))
                                .add(Projection.of("u", "x"))
                                .build(), null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(@Nonnull String sparql, boolean strict, @Nullable Object expectedObj,
                          Class<? extends Throwable> exception) throws Exception {
        SPARQLParser parser = strict ? SPARQLParser.strict()
                                     : SPARQLParser.tolerant();
        if (exception == null) {
            Op query = parser.parse(sparql);
            Op expected = null;
            if (expectedObj instanceof CQuery)
                expected = new QueryOp((CQuery) expectedObj);
            else if (expectedObj instanceof Op)
                expected = (Op)expectedObj;
            else
                fail("Neither a CQuery nor an Op: expectedObj="+expectedObj);
            assertNotNull(expected); // bad test data
            assertEquals(query.getMatchedTriples(), expected.getMatchedTriples());
            assertEquals(query.modifiers(), expected.modifiers());

            //compare modifiers
            Set<Modifier> expectedModifiers = TreeUtils.streamPreOrder(expected)
                    .flatMap(o -> o.modifiers().stream()).collect(toSet());
            Set<Modifier> actualModifiers = TreeUtils.streamPreOrder(query)
                    .flatMap(o -> o.modifiers().stream()).collect(toSet());
            assertEquals(actualModifiers, expectedModifiers);

            //compare variables in root
            assertEquals(query.getResultVars(), expected.getResultVars());
            assertEquals(query.getStrictResultVars(), expected.getStrictResultVars());
            assertEquals(query.getInputVars(), expected.getInputVars());
            assertEquals(query.getRequiredInputVars(), expected.getRequiredInputVars());
            assertEquals(query.getOptionalInputVars(), expected.getOptionalInputVars());

            // compare whole tree (including variables and modifiers)
            //noinspection SimplifiedTestNGAssertion
            assertTrue(query.equals(expected));

            assertTripleUniverse(query);
            assertVarsUniverse(query);

            if (strict) // if strict was OK, tolerant should also be ok with it
                testParse(sparql, false, expected, null);
        } else {
            assertNull(expectedObj); //bad test data
            expectThrows(exception, () -> parser.parse(sparql));
        }
    }

    @Test(enabled = false) //not a test
    public static void assertUniverses(@Nonnull Op root) {
        assertTripleUniverse(root);
        assertVarsUniverse(root);
    }

    @Test(enabled = false) //not a test
    public static void assertTripleUniverse(@Nonnull Op root) {
        checkCQueryTriplesParent(root);
        checkCQueryMatchedTriplesParent(root);
        checkMatchedTriplesParent(root);
        checkTriplesSetParent(root);
    }

    @Test(enabled = false) //not a test
    public static void assertVarsUniverse(@Nonnull Op root) {
        checkCQueryVarNameSetsParent(root);
        checkOpVarNameSetsParent(root);
    }

    private static class ParentChecker
            implements java.util.function.Consumer<Set<?>> {

        private IndexSet<?> expected = null;
        @Override public void accept(Set<?> set) {
            assertTrue(set instanceof IndexSet);
            assertTrue(set instanceof IndexSubset);
            IndexSet<?> parent = ((IndexSubset<?>) set).getParent();
            if (expected == null) expected = parent;
            else                  assertSame(parent, expected);
        }

    }

    private static void checkCQueryTriplesParent(Op root) {
        Iterator<IndexSet<Triple>> it = TreeUtils.streamPreOrder(root)
                .filter(QueryOp.class::isInstance)
                .map(o -> ((QueryOp) o).getQuery().attr().getSet()).iterator();
        ParentChecker checker = new ParentChecker();
        while (it.hasNext())
            checker.accept(it.next());
    }

    private static void checkCQueryMatchedTriplesParent(Op root) {
        Iterator<IndexSet<Triple>> it = TreeUtils.streamPreOrder(root)
                .filter(QueryOp.class::isInstance)
                .map(o -> ((QueryOp) o).getQuery().attr().matchedTriples()).iterator();
        ParentChecker checker = new ParentChecker();
        while (it.hasNext())
            checker.accept(it.next());
    }

    private static void checkCQueryVarNameSetsParent(Op root) {
        Iterator<MutableCQuery> it;
        it = TreeUtils.streamPreOrder(root).filter(QueryOp.class::isInstance)
                .map(o -> ((QueryOp) o).getQuery()).iterator();
        ParentChecker checker = new ParentChecker();
        while (it.hasNext()) {
            CQuery q = it.next();
            checker.accept(q.attr().allVarNames());
            checker.accept(q.attr().tripleVarNames());
            checker.accept(q.attr().publicVarNames());
            checker.accept(q.attr().publicTripleVarNames());
            checker.accept(q.attr().reqInputVarNames());
            checker.accept(q.attr().optInputVarNames());
            checker.accept(q.attr().inputVarNames());
        }
    }

    private static void checkOpVarNameSetsParent(Op root) {
        ParentChecker checker = new ParentChecker();
        for (Iterator<Op> it = TreeUtils.iteratePreOrder(root); it.hasNext(); ) {
            Op op = it.next();
            checker.accept(op.getResultVars());
            checker.accept(op.getStrictResultVars());
            checker.accept(op.getRequiredInputVars());
            checker.accept(op.getOptionalInputVars());
            checker.accept(op.getInputVars());
            checker.accept(op.getPublicVars());
            checker.accept(op.getAllVars());
        }
    }

    private static void checkTriplesSetParent(Op query) {
        IndexSet<Triple> universe = null;
        Iterator<MutableCQuery> it = TreeUtils.streamPreOrder(query)
                .filter(QueryOp.class::isInstance).map(o -> ((QueryOp) o).getQuery()).iterator();
        while (it.hasNext()) {
            IndexSet<Triple> set = it.next().attr().getSet();
            assertTrue(set instanceof IndexSubset);
            if (universe == null) universe = set.getParent();
            else                  assertSame(set.getParent(), universe);
        }
    }

    private static void checkMatchedTriplesParent(Op query) {
        IndexSet<Triple> universe = null;
        for (Iterator<Op> it = TreeUtils.iteratePreOrder(query); it.hasNext(); ) {
            Op op = it.next();
            if (op instanceof QueryOp) {
                IndexSet<Triple> set = ((QueryOp) op).getQuery().attr().getSet();
                assertTrue(set instanceof IndexSubset);
                IndexSet<Triple> parent = set.getParent();
                if (universe == null) universe = parent;
                else                         assertSame(parent, universe);
            }
            Set<Triple> matched = op.getMatchedTriples();
            assertTrue(matched instanceof IndexSubset);
            IndexSet<Triple> parent = ((IndexSubset<Triple>) matched).getParent();
            if (universe == null) universe = parent;
            else                         assertSame(parent, universe);
        }
    }
}