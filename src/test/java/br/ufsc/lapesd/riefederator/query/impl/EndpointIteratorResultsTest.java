package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.*;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class EndpointIteratorResultsTest {
    private static final @Nonnull URI ALICE = new StdURI("http://example.org/Alice");
    private static final @Nonnull URI BOB = new StdURI("http://example.org/Bob");
    private static final @Nonnull URI DAVE = new StdURI("http://example.org/Dave");
    private static final @Nonnull URI knows = new StdURI(FOAF.knows.getURI());
    private static final @Nonnull Var X = new StdVar("x");
    private static final @Nonnull Var Y = new StdVar("y");

    private @Nullable CQEndpoint e1, e2, rdf1, rdf2;

    private ARQEndpoint createEndpoint(String resourcePath) {
        Model model = ModelFactory.createDefaultModel();
        InputStream in = getClass().getResourceAsStream(resourcePath);
        RDFDataMgr.read(model, in, Lang.TTL);
        return ARQEndpoint.forModel(model);
    }

    @BeforeMethod
    public void setUp() {
        e1 = new EmptyEndpoint();
        e2 = new EmptyEndpoint();
        rdf1 = createEndpoint("../../rdf-1.nt");
        rdf2 = createEndpoint("../../rdf-2.nt");
    }

    @AfterMethod
    public void tearDown() {
        e1 = rdf1 = rdf2 = null;
    }

    @Test
    public void testNoEndpoints() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        try (Results results = new EndpointIteratorResults(emptyIterator(), query)) {
            assertFalse(results.hasNext());
            expectThrows(NoSuchElementException.class, results::next);
            assertEquals(results.getReadyCount(), 0);
            assertEquals(results.getCardinality(), Cardinality.UNSUPPORTED);
        }
    }

    @Test
    public void testEmptyEndpoints() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        try (Results results = new EndpointIteratorResults(singleton(e1).iterator(), query)) {
            assertFalse(results.hasNext());
            assertEquals(results.getCardinality(), Cardinality.EMPTY);
        }
        try (Results results = new EndpointIteratorResults(asList(e1, e2).iterator(), query)) {
            assertFalse(results.hasNext());
            assertEquals(results.getCardinality(), Cardinality.EMPTY);
        }
    }

    @Test
    public void testSingleEndpoint() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        Set<Solution> actual = new HashSet<>();
        try (Results results = new EndpointIteratorResults(singleton(rdf1).iterator(), query)) {
            assertEquals(results.getReadyCount(), 0);
            assertTrue(results.hasNext());
            assertEquals(results.getReadyCount(), 1);
            results.forEachRemaining(actual::add);
        }
        assertEquals(actual, singleton(MapSolution.builder().put(X, ALICE).put(Y, BOB).build()));
    }


    @Test
    public void testEmptyEndpointsInBetween() {
        CQuery qry = CQuery.from(new Triple(X, knows, BOB));
        Set<Solution> actual = new HashSet<>();
        try (Results results = new EndpointIteratorResults(asList(e1, rdf2, e2).iterator(), qry)) {
            assertEquals(results.getReadyCount(), 0);
            results.forEachRemaining(actual::add);
            assertEquals(results.getCardinality(), Cardinality.EMPTY);
        }
        assertEquals(actual, Sets.newHashSet(MapSolution.build(X, ALICE),
                                             MapSolution.build(X, DAVE)));
    }

    @Test
    public void testAllEndpoints() {
        CQuery query = CQuery.from(new Triple(X, knows, BOB));
        List<CQEndpoint> eps = asList(e1, rdf2, e2, rdf1);
        try (Results results = new EndpointIteratorResults(eps.iterator(), query)) {
            assertEquals(results.getReadyCount(), 0);
            assertTrue(results.hasNext());
            Set<Solution> set = new HashSet<>();
            set.add(results.next());
            set.add(results.next());
            assertEquals(set, Sets.newHashSet(MapSolution.build(X, ALICE),
                                              MapSolution.build(X, DAVE)));
            assertTrue(results.hasNext());
            assertEquals(results.next(), MapSolution.build(X, ALICE));
            assertFalse(results.hasNext());
            assertEquals(results.getCardinality(), Cardinality.EMPTY);
        }
    }

    @Test
    public void testBadProjection() {
        CQuery query = CQuery.from(new Triple(X, knows, BOB));
        HashSet<String> xy = Sets.newHashSet("x", "y");
        expectThrows(IllegalArgumentException.class,
                () -> new EndpointIteratorResults(emptyIterator(), query, xy, true));
    }


    @Test
    public void testDeclareProjectionNotProjecting() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        HashSet<String> xy = Sets.newHashSet("x", "y");
        expectThrows(IllegalArgumentException.class,
                () -> new EndpointIteratorResults(emptyIterator(), query, xy, true));
    }

    @Test
    public void testProjectNotDeclaring() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        Set<String> x = singleton("x");
        expectThrows(IllegalArgumentException.class,
                () -> new EndpointIteratorResults(emptyIterator(), query, x, false));
    }

    @Test
    public void testProject() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        Iterator<CQEndpoint> iterator = asList(rdf1, e1, rdf2).iterator();
        Set<String> x = singleton("x");
        try (Results results = new EndpointIteratorResults(iterator, query, x, true)) {
            assertTrue(results.hasNext());
            assertEquals(results.next(), MapSolution.build(X, ALICE));
            assertTrue(results.hasNext());

            Set<Solution> set = new HashSet<>();
            set.add(results.next());
            set.add(results.next());
            assertEquals(set, Sets.newHashSet(MapSolution.build(X, ALICE),
                                              MapSolution.build(X, DAVE)));
            assertFalse(results.hasNext());
        }
    }

}