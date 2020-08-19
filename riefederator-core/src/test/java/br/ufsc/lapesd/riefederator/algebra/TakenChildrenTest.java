package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class TakenChildrenTest implements TestContext {

    private static @Nonnull Op qn(Object... args) {
        return new QueryOp(createQuery(args));
    }

    private static @Nonnull InnerOp parent(Op... children) {
        List<Op> list = new ArrayList<>();
        for (Op child : children)
            list.add(child);
        return new ConjunctionOp(list);
    }

    @Test
    public void testGetChildren() {
        Op n1 = qn(Alice, knows, x), n2 = qn(x, knows, y);
        InnerOp parent = parent(n1, n2);
        TakenChildren children = parent.takeChildren();
        assertSame(children.get(0), n1);
        assertSame(children.get(1), n2);
        assertEquals(children.size(), 2);
    }

    @Test
    public void testReplaceChild() {
        Op n2 = qn(x, knows, y);
        Op replacement = qn(Bob, knows, x);
        InnerOp parent = parent(qn(Alice, knows, x), n2);
        try (TakenChildren children = parent.takeChildren()) {
            assertFalse(children.hasRefsChange());
            assertEquals(children.set(1, replacement), n2);
            assertTrue(children.hasRefsChange());
        }
        assertSame(parent.getChildren().get(1), replacement);
    }

    @Test
    public void testRemoveByIterator() {
        Op n1 = qn(Alice, knows, x), n2 = qn(x, knows, y);
        InnerOp parent = parent(n1, n2);
        try (TakenChildren children = parent.takeChildren()) {
            assertFalse(children.hasRefsChange());
            Iterator<Op> it = children.iterator();
            assertTrue(it.hasNext());
            assertSame(it.next(), n1);
            assertFalse(children.hasRefsChange());
            it.remove();
            assertTrue(children.hasRefsChange());
            assertEquals(children, Collections.singletonList(n2));
            assertSame(children.get(0), n2);
        }
        assertEquals(parent.getChildren(), Collections.singletonList(n2));
        assertSame(parent.getChildren().get(0), n2);
    }

    @Test
    public void testSetByIterator() {
        Op n1 = qn(Alice, knows, x), n2 = qn(x, knows, y);
        Op replacement = qn(x, knows, z);
        InnerOp parent = parent(n1, n2);
        try (TakenChildren children = parent.takeChildren()) {
            ListIterator<Op> it = children.listIterator();
            assertTrue(it.hasNext());
            assertSame(it.next(), n1);
            assertTrue(it.hasNext());
            assertSame(it.next(), n2);
            assertFalse(children.hasRefsChange());
            it.set(replacement);
            assertTrue(children.hasRefsChange());
            assertEquals(children.size(), 2);
            assertSame(children.get(0), n1);
            assertSame(children.get(1), replacement);
        }
        assertEquals(parent.getChildren().size(), 2);
        assertSame(parent.getChildren().get(0), n1);
        assertSame(parent.getChildren().get(1), replacement);
    }

    @Test
    public void testReverseListIterator() {
        Op n1 = qn(Alice, knows, x), n2 = qn(x, knows, y);
        Op replacement = qn(Alice, knows, y);
        InnerOp parent = parent(n1, n2);
        try (TakenChildren children = parent.takeChildren()) {
            ListIterator<Op> it = children.listIterator(children.size());
            assertTrue(children.hasContentChange());
            assertFalse(children.hasRefsChange());
            assertTrue(it.hasPrevious());
            assertSame(it.previous(), n2);
            assertTrue(it.hasPrevious());
            assertEquals(it.previousIndex(), 0);
            assertEquals(it.previousIndex(), 0); // does not change iterator state
            assertSame(it.previous(), n1);
            it.set(replacement);
            assertTrue(children.hasRefsChange());
            assertEquals(children.size(), 2);
            assertSame(children.get(0), replacement);
            assertSame(children.get(1), n2);
            assertTrue(children.hasContentChange());
        }
        assertEquals(parent.getChildren().size(), 2);
        assertSame(parent.getChildren().get(0), replacement);
        assertSame(parent.getChildren().get(1), n2);
    }

    @Test
    public void testSuppressContentChange() {
        Op n1 = qn(Alice, knows, x), n2 = qn(Alice, knows, x);
        InnerOp parent = parent(n1);
        try (TakenChildren children = parent.takeChildren()) {
            assertTrue(children.hasContentChange());
            assertFalse(children.hasRefsChange());
            children.setNoContentChange();
            assertFalse(children.hasContentChange());
            assertTrue(children.add(n2));
            assertFalse(children.hasContentChange());
            assertTrue(children.hasRefsChange());
            assertEquals(children.size(), 2);
            assertEquals(children.get(0), n1);
            assertEquals(children.get(1), n2);
        }
        assertEquals(parent.getChildren().size(), 2);
        assertEquals(parent.getChildren().get(0), n1);
        assertEquals(parent.getChildren().get(1), n2);
    }

    @Test
    public void testAssertOnClose() {
        InnerOp parent = parent(qn(Alice, knows, x), qn(x, knows, y));
        expectThrows(AssertionError.class, () -> {
            try (TakenChildren children = parent.takeChildren()) {
                children.set(1, qn(Bob, knows, x));
                children.setNoContentChange();
            } // will throw from children.close()
        });
    }

}