package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Set;

import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class SPARQLAssert {
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
}
