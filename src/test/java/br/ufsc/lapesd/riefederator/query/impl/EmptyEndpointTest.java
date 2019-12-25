package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Results;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class EmptyEndpointTest {
    public static final @Nonnull StdURI ALICE = new StdURI("https://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("https://example.org/Bob");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");

    @Test
    public void testNoVars() {
        CQuery q = CQuery.from(new Triple(ALICE, knows, BOB));
        EmptyEndpoint ep = new EmptyEndpoint();
        try (Results results = ep.query(q)) {
            assertFalse(results.hasNext());
            assertEquals(results.getVarNames(), emptySet());
        }
    }

    @Test
    public void testSingleTripleVars() {
        CQuery q = CQuery.from(new Triple(ALICE, knows, X));
        EmptyEndpoint ep = new EmptyEndpoint();
        try (Results results = ep.query(q)) {
            assertFalse(results.hasNext());
            assertEquals(results.getVarNames(), singleton("x"));
        }
    }

    @Test
    public void testDistinctWithVars() {
        CQuery q = CQuery.from(asList(new Triple(ALICE, knows, X),
                                      new Triple(X, knows, Y)));
        EmptyEndpoint ep = new EmptyEndpoint();
        try (Results results = ep.query(q)) {
            assertFalse(results.hasNext());
            assertEquals(results.getVarNames(), Sets.newHashSet("x", "y"));
        }
    }
}