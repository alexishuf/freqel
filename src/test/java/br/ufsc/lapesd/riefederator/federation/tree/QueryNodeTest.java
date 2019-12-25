package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class QueryNodeTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bon");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    private static final EmptyEndpoint empty = new EmptyEndpoint();

    @Test
    public void testNoVars() {
        CQuery query = CQuery.from(new Triple(ALICE, knows, BOB));
        QueryNode node = new QueryNode(empty, query);
        assertSame(node.getEndpoint(), empty);
        assertSame(node.getQuery(), query);
        assertEquals(node.getResultVars(), emptySet());
        assertFalse(node.isProjecting());
    }

    @Test
    public void testVarsInTriple() {
        QueryNode node = new QueryNode(empty, CQuery.from(new Triple(ALICE, knows, X)));
        assertEquals(node.getResultVars(), singleton("x"));
        assertFalse(node.isProjecting());
    }

    @Test
    public void testVarsInConjunctive() {
        CQuery query = CQuery.from(asList(new Triple(ALICE, knows, X),
                                          new Triple(X, knows, Y)));
        QueryNode node = new QueryNode(empty, query);
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertFalse(node.isProjecting());
        assertFalse(node.toString().startsWith("π"));
    }


    @Test
    public void testVarsInProjection() {
        CQuery query = CQuery.from(asList(new Triple(ALICE, knows, X),
                                          new Triple(X, knows, Y)));
        QueryNode node = new QueryNode(empty, query, singleton("x"));
        assertEquals(node.getResultVars(), singleton("x"));
        assertTrue(node.isProjecting());
        assertTrue(node.toString().startsWith("π[x]("));
    }

}