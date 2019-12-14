package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.riefederator.query.JoinType.*;
import static org.testng.Assert.*;

public class JoinTypeTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");

    public static final @Nonnull Triple    BOUND = new Triple(ALICE, KNOWS, BOB);
    public static final @Nonnull Triple S_TRIPLE = new Triple(X,     KNOWS, BOB);
    public static final @Nonnull Triple P_TRIPLE = new Triple(ALICE, X,     BOB);
    public static final @Nonnull Triple O_TRIPLE = new Triple(ALICE, KNOWS, X  );

    protected Term[] forEachSourceAt(@Nonnull JoinType type, @Nonnull Triple triple) {
        Term[] terms = new Term[3];
        type.forEachSourceAt(triple, (t, p) -> terms[p.ordinal()] = t);
        return terms;
    }

    protected Term[] forEachDestinationAt(@Nonnull JoinType type, @Nonnull Triple triple) {
        Term[] terms = new Term[3];
        type.forEachDestinationAt(triple, (t, p) -> terms[p.ordinal()] = t);
        return terms;
    }

    @Test
    public void testForEachANY() {
        assertEquals(forEachSourceAt(ANY, BOUND), new Term[]{ALICE, KNOWS, BOB});
        assertEquals(forEachSourceAt(ANY, S_TRIPLE), new Term[]{X, KNOWS, BOB});
        assertEquals(forEachDestinationAt(ANY, BOUND), new Term[]{ALICE, KNOWS, BOB});
        assertEquals(forEachDestinationAt(ANY, S_TRIPLE), new Term[]{X, KNOWS, BOB});
    }

    @Test
    public void testForEachVARS() {
        assertEquals(forEachSourceAt(VARS, BOUND), new Term[]{null, null, null});
        assertEquals(forEachSourceAt(VARS, S_TRIPLE), new Term[]{X, null, null});
        assertEquals(forEachSourceAt(VARS, P_TRIPLE), new Term[]{null, X, null});
        assertEquals(forEachDestinationAt(VARS, BOUND), new Term[]{null, null, null});
        assertEquals(forEachDestinationAt(VARS, S_TRIPLE), new Term[]{X, null, null});
        assertEquals(forEachDestinationAt(VARS, P_TRIPLE), new Term[]{null, X, null});
    }

    @Test
    public void testForEachSUBJ_OBJ() {
        assertEquals(forEachSourceAt(SUBJ_OBJ, BOUND), new Term[]{ALICE, null, null});
        assertEquals(forEachSourceAt(SUBJ_OBJ, S_TRIPLE), new Term[]{X, null, null});
        assertEquals(forEachSourceAt(SUBJ_OBJ, O_TRIPLE), new Term[]{ALICE, null, null});

        assertEquals(forEachDestinationAt(SUBJ_OBJ, BOUND), new Term[]{null, null, BOB});
        assertEquals(forEachDestinationAt(SUBJ_OBJ, S_TRIPLE), new Term[]{null, null, BOB});
        assertEquals(forEachDestinationAt(SUBJ_OBJ, O_TRIPLE), new Term[]{null, null, X});
    }

    @Test
    public void testForEachOBJ_SUBJ() {
        assertEquals(forEachSourceAt(OBJ_SUBJ, BOUND), new Term[]{null, null, BOB});
        assertEquals(forEachSourceAt(OBJ_SUBJ, O_TRIPLE), new Term[]{null, null, X});

        assertEquals(forEachDestinationAt(OBJ_SUBJ, BOUND), new Term[]{ALICE, null, null});
        assertEquals(forEachDestinationAt(OBJ_SUBJ, S_TRIPLE), new Term[]{X, null, null});
    }

    @Test
    public void testForEachSUBJ_SUBJ() {
        assertEquals(forEachSourceAt(SUBJ_SUBJ, BOUND), new Term[]{ALICE, null, null});
        assertEquals(forEachSourceAt(SUBJ_SUBJ, O_TRIPLE), new Term[]{ALICE, null, null});
        assertEquals(forEachSourceAt(SUBJ_SUBJ, S_TRIPLE), new Term[]{X, null, null});
        assertEquals(forEachDestinationAt(SUBJ_SUBJ, BOUND), new Term[]{ALICE, null, null});
        assertEquals(forEachDestinationAt(SUBJ_SUBJ, O_TRIPLE), new Term[]{ALICE, null, null});
        assertEquals(forEachDestinationAt(SUBJ_SUBJ, S_TRIPLE), new Term[]{X, null, null});
    }

    @Test
    public void testAllowDestination() {
        assertTrue(ANY.allowDestination(X, S_TRIPLE));
        assertTrue(ANY.allowDestination(X, O_TRIPLE));
        assertTrue(ANY.allowDestination(KNOWS, BOUND));
        assertTrue(VARS.allowDestination(X, S_TRIPLE));
        assertTrue(VARS.allowDestination(X, P_TRIPLE));
        assertFalse(VARS.allowDestination(Y, P_TRIPLE));

        expectThrows(IllegalArgumentException.class, () -> VARS.allowDestination(ALICE, BOUND));
        expectThrows(IllegalArgumentException.class, () -> VARS.allowDestination(KNOWS, S_TRIPLE));

        assertTrue(OBJ_SUBJ.allowDestination(ALICE, BOUND));
        assertFalse(OBJ_SUBJ.allowDestination(BOB,  BOUND));
        assertFalse(OBJ_SUBJ.allowDestination(X,    BOUND));
    }

    @Test
    public void testAllowDestinationValidatingSource() {
        assertTrue(ANY.allowDestination(ALICE, Triple.Position.SUBJ, BOUND));
        assertTrue(ANY.allowDestination(X, Triple.Position.OBJ, O_TRIPLE));

        assertTrue(VARS.allowDestination(X, Triple.Position.SUBJ, S_TRIPLE));
        assertTrue(VARS.allowDestination(X, Triple.Position.PRED, P_TRIPLE));
        assertTrue(VARS.allowDestination(X, Triple.Position.OBJ, S_TRIPLE));
        assertFalse(VARS.allowDestination(ALICE, Triple.Position.SUBJ, P_TRIPLE));

        assertTrue(OBJ_SUBJ.allowDestination(X, Triple.Position.OBJ, S_TRIPLE));
        assertFalse(OBJ_SUBJ.allowDestination(X, Triple.Position.SUBJ, S_TRIPLE));
        assertFalse(OBJ_SUBJ.allowDestination(X, Triple.Position.OBJ, BOUND));
        assertFalse(OBJ_SUBJ.allowDestination(Y, Triple.Position.OBJ, S_TRIPLE));
    }
}