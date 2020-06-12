package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Results;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(groups = {"fast"})
public class EmptyEndpointTest implements TestContext {

    @Test
    public void testNoVars() {
        CQuery q = CQuery.from(new Triple(Alice, knows, Bob));
        EmptyEndpoint ep = new EmptyEndpoint();
        try (Results results = ep.query(q)) {
            assertFalse(results.hasNext());
            assertEquals(results.getVarNames(), emptySet());
        }
    }

    @Test
    public void testSingleTripleVars() {
        CQuery q = CQuery.from(new Triple(Alice, knows, x));
        EmptyEndpoint ep = new EmptyEndpoint();
        try (Results results = ep.query(q)) {
            assertFalse(results.hasNext());
            assertEquals(results.getVarNames(), singleton("x"));
        }
    }

    @Test
    public void testDistinctWithVars() {
        CQuery q = CQuery.from(asList(new Triple(Alice, knows, x),
                                      new Triple(x, knows, y)));
        EmptyEndpoint ep = new EmptyEndpoint();
        try (Results results = ep.query(q)) {
            assertFalse(results.hasNext());
            assertEquals(results.getVarNames(), Sets.newHashSet("x", "y"));
        }
    }
}