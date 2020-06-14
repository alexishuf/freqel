package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.EndpointIteratorResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class EndpointIteratorResultsTest implements TestContext {
    private @Nullable
    CQEndpoint e1, e2, rdf1, rdf2;

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
        CQuery query = CQuery.from(new Triple(x, knows, y));
        try (Results results = new EndpointIteratorResults(emptyIterator(), query)) {
            assertFalse(results.hasNext());
            expectThrows(NoSuchElementException.class, results::next);
            assertEquals(results.getReadyCount(), 0);
        }
    }

    @Test
    public void testEmptyEndpoints() {
        CQuery query = CQuery.from(new Triple(x, knows, y));
        try (Results results = new EndpointIteratorResults(singleton(e1).iterator(), query)) {
            assertFalse(results.hasNext());
        }
        try (Results results = new EndpointIteratorResults(asList(e1, e2).iterator(), query)) {
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void testSingleEndpoint() {
        CQuery query = CQuery.from(new Triple(x, knows, y));
        Set<Solution> actual = new HashSet<>();
        try (Results results = new EndpointIteratorResults(singleton(rdf1).iterator(), query)) {
            assertEquals(results.getReadyCount(), 0);
            assertTrue(results.hasNext());
            assertEquals(results.getReadyCount(), 1);
            results.forEachRemaining(actual::add);
        }
        assertEquals(actual, singleton(MapSolution.builder().put(x, Alice).put(y, Bob).build()));
    }


    @Test
    public void testEmptyEndpointsInBetween() {
        CQuery qry = CQuery.from(new Triple(x, knows, Bob));
        Set<Solution> actual = new HashSet<>();
        try (Results results = new EndpointIteratorResults(asList(e1, rdf2, e2).iterator(), qry)) {
            assertEquals(results.getReadyCount(), 0);
            results.forEachRemaining(actual::add);
        }
        assertEquals(actual, Sets.newHashSet(MapSolution.build(x, Alice),
                                             MapSolution.build(x, Dave)));
    }

    @Test
    public void testAllEndpoints() {
        CQuery query = CQuery.from(new Triple(x, knows, Bob));
        List<CQEndpoint> eps = asList(e1, rdf2, e2, rdf1);
        try (Results results = new EndpointIteratorResults(eps.iterator(), query)) {
            assertEquals(results.getReadyCount(), 0);
            assertTrue(results.hasNext());
            Set<Solution> set = new HashSet<>();
            set.add(results.next());
            set.add(results.next());
            assertEquals(set, Sets.newHashSet(MapSolution.build(x, Alice),
                                              MapSolution.build(x, Dave)));
            assertTrue(results.hasNext());
            assertEquals(results.next(), MapSolution.build(x, Alice));
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void testBadProjection() {
        CQuery query = CQuery.from(new Triple(x, knows, Bob));
        HashSet<String> xy = Sets.newHashSet("x", "y");
        expectThrows(IllegalArgumentException.class,
                () -> new EndpointIteratorResults(emptyIterator(), query, xy, true));
    }


    @Test
    public void testDeclareProjectionNotProjecting() {
        CQuery query = CQuery.from(new Triple(x, knows, y));
        HashSet<String> xy = Sets.newHashSet("x", "y");
        expectThrows(IllegalArgumentException.class,
                () -> new EndpointIteratorResults(emptyIterator(), query, xy, true));
    }

    @Test
    public void testProjectNotDeclaring() {
        CQuery query = CQuery.from(new Triple(x, knows, y));
        Set<String> x = singleton("x");
        expectThrows(IllegalArgumentException.class,
                () -> new EndpointIteratorResults(emptyIterator(), query, x, false));
    }

    @Test
    public void testProject() {
        CQuery query = CQuery.from(new Triple(x, knows, y));
        Iterator<CQEndpoint> iterator = asList(rdf1, e1, rdf2).iterator();
        Set<String> x = singleton("x");
        try (Results results = new EndpointIteratorResults(iterator, query, x, true)) {
            assertTrue(results.hasNext());
            assertEquals(results.next(), MapSolution.build(EndpointIteratorResultsTest.x, Alice));
            assertTrue(results.hasNext());

            Set<Solution> set = new HashSet<>();
            set.add(results.next());
            set.add(results.next());
            assertEquals(set, Sets.newHashSet(MapSolution.build(EndpointIteratorResultsTest.x, Alice),
                                              MapSolution.build(EndpointIteratorResultsTest.x, Dave)));
            assertFalse(results.hasNext());
        }
    }

}