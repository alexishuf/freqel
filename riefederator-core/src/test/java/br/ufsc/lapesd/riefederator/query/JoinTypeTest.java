package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.riefederator.query.JoinType.*;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class JoinTypeTest  implements TestContext {
    public static final @Nonnull Triple    BOUND = new Triple(Alice, knows, Bob);
    public static final @Nonnull Triple S_TRIPLE = new Triple(x, knows, Bob);
    public static final @Nonnull Triple P_TRIPLE = new Triple(Alice, x, Bob);
    public static final @Nonnull Triple O_TRIPLE = new Triple(Alice, knows, x);

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
        assertEquals(forEachSourceAt(ANY, BOUND), new Term[]{Alice, knows, Bob});
        assertEquals(forEachSourceAt(ANY, S_TRIPLE), new Term[]{x, knows, Bob});
        assertEquals(forEachDestinationAt(ANY, BOUND), new Term[]{Alice, knows, Bob});
        assertEquals(forEachDestinationAt(ANY, S_TRIPLE), new Term[]{x, knows, Bob});
    }

    @Test
    public void testForEachVARS() {
        assertEquals(forEachSourceAt(VARS, BOUND), new Term[]{null, null, null});
        assertEquals(forEachSourceAt(VARS, S_TRIPLE), new Term[]{x, null, null});
        assertEquals(forEachSourceAt(VARS, P_TRIPLE), new Term[]{null, x, null});
        assertEquals(forEachDestinationAt(VARS, BOUND), new Term[]{null, null, null});
        assertEquals(forEachDestinationAt(VARS, S_TRIPLE), new Term[]{x, null, null});
        assertEquals(forEachDestinationAt(VARS, P_TRIPLE), new Term[]{null, x, null});
    }

    @Test
    public void testForEachSUBJ_OBJ() {
        assertEquals(forEachSourceAt(SUBJ_OBJ, BOUND), new Term[]{Alice, null, null});
        assertEquals(forEachSourceAt(SUBJ_OBJ, S_TRIPLE), new Term[]{x, null, null});
        assertEquals(forEachSourceAt(SUBJ_OBJ, O_TRIPLE), new Term[]{Alice, null, null});

        assertEquals(forEachDestinationAt(SUBJ_OBJ, BOUND), new Term[]{null, null, Bob});
        assertEquals(forEachDestinationAt(SUBJ_OBJ, S_TRIPLE), new Term[]{null, null, Bob});
        assertEquals(forEachDestinationAt(SUBJ_OBJ, O_TRIPLE), new Term[]{null, null, x});
    }

    @Test
    public void testForEachOBJ_SUBJ() {
        assertEquals(forEachSourceAt(OBJ_SUBJ, BOUND), new Term[]{null, null, Bob});
        assertEquals(forEachSourceAt(OBJ_SUBJ, O_TRIPLE), new Term[]{null, null, x});

        assertEquals(forEachDestinationAt(OBJ_SUBJ, BOUND), new Term[]{Alice, null, null});
        assertEquals(forEachDestinationAt(OBJ_SUBJ, S_TRIPLE), new Term[]{x, null, null});
    }

    @Test
    public void testForEachSUBJ_SUBJ() {
        assertEquals(forEachSourceAt(SUBJ_SUBJ, BOUND), new Term[]{Alice, null, null});
        assertEquals(forEachSourceAt(SUBJ_SUBJ, O_TRIPLE), new Term[]{Alice, null, null});
        assertEquals(forEachSourceAt(SUBJ_SUBJ, S_TRIPLE), new Term[]{x, null, null});
        assertEquals(forEachDestinationAt(SUBJ_SUBJ, BOUND), new Term[]{Alice, null, null});
        assertEquals(forEachDestinationAt(SUBJ_SUBJ, O_TRIPLE), new Term[]{Alice, null, null});
        assertEquals(forEachDestinationAt(SUBJ_SUBJ, S_TRIPLE), new Term[]{x, null, null});
    }

    @Test
    public void testAllowDestination() {
        assertTrue(ANY.allowDestination(x, S_TRIPLE));
        assertTrue(ANY.allowDestination(x, O_TRIPLE));
        assertTrue(ANY.allowDestination(knows, BOUND));
        assertTrue(VARS.allowDestination(x, S_TRIPLE));
        assertTrue(VARS.allowDestination(x, P_TRIPLE));
        assertFalse(VARS.allowDestination(y, P_TRIPLE));

        expectThrows(IllegalArgumentException.class, () -> VARS.allowDestination(Alice, BOUND));
        expectThrows(IllegalArgumentException.class, () -> VARS.allowDestination(knows, S_TRIPLE));

        assertTrue(OBJ_SUBJ.allowDestination(Alice, BOUND));
        assertFalse(OBJ_SUBJ.allowDestination(Bob,  BOUND));
        assertFalse(OBJ_SUBJ.allowDestination(x,    BOUND));
    }

    @Test
    public void testAllowDestinationValidatingSource() {
        assertTrue(ANY.allowDestination(Alice, Triple.Position.SUBJ, BOUND));
        assertTrue(ANY.allowDestination(x, Triple.Position.OBJ, O_TRIPLE));

        assertTrue(VARS.allowDestination(x, Triple.Position.SUBJ, S_TRIPLE));
        assertTrue(VARS.allowDestination(x, Triple.Position.PRED, P_TRIPLE));
        assertTrue(VARS.allowDestination(x, Triple.Position.OBJ, S_TRIPLE));
        assertFalse(VARS.allowDestination(Alice, Triple.Position.SUBJ, P_TRIPLE));

        assertTrue(OBJ_SUBJ.allowDestination(x, Triple.Position.OBJ, S_TRIPLE));
        assertFalse(OBJ_SUBJ.allowDestination(x, Triple.Position.SUBJ, S_TRIPLE));
        assertFalse(OBJ_SUBJ.allowDestination(x, Triple.Position.OBJ, BOUND));
        assertFalse(OBJ_SUBJ.allowDestination(y, Triple.Position.OBJ, S_TRIPLE));
    }
}