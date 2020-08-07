package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class ModifiersSetTest implements TestContext {

    @SuppressWarnings({"WhileLoopReplaceableByForEach", "UseBulkOperation"})
    @Test
    public void testAllSubsets() {
        Set<Modifier> all = Sets.newHashSet(
                Projection.required("x"),
                Ask.REQUIRED,
                Limit.required(23),
                new ValuesModifier(singleton("x"), singleton(MapSolution.build(x, Alice))),
                SPARQLFilter.build("?u > 23"),
                SPARQLFilter.build("?v > 23")
        );
        for (Set<Modifier> subset : Sets.powerSet(all)) {
            ModifiersSet set = new ModifiersSet();
            for (Modifier m : subset)
                assertTrue(set.add(m));
            assertEquals(set.size(), subset.size());
            assertEquals(set.isEmpty(), subset.isEmpty());

            ArrayList<Modifier> reverse = new ArrayList<>(subset);
            Collections.reverse(reverse);
            for (Modifier modifier : reverse) {
                assertTrue(set.contains(modifier));
                assertFalse(set.add(modifier));
            }
            assertEquals(set.size(), subset.size());

            for (Modifier modifier : subset)
                assertTrue(set.contains(modifier));
            assertFalse(set.addAll(subset));
            assertFalse(set.addAll(reverse));

            for (Modifier modifier : reverse)
                assertTrue(set.remove(modifier));
            assertEquals(set.size(), 0);
            assertTrue(set.isEmpty());

            assertEquals(set.addAll(reverse), !subset.isEmpty());
            assertFalse(set.addAll(subset));
            assertEquals(set.size(), subset.size());
            assertTrue(set.containsAll(subset));

            assertEquals(set.removeAll(subset), !subset.isEmpty());
            assertEquals(set.size(), 0);
            assertTrue(set.isEmpty());

            assertEquals(set.addAll(reverse), !subset.isEmpty());
            assertTrue(set.containsAll(reverse));
            assertTrue(set.containsAll(subset));

            Set<Modifier> iteration = new HashSet<>();
            Iterator<Modifier> it = set.iterator();
            while (it.hasNext())
                iteration.add(it.next());
            assertEquals(iteration, subset);

            assertEquals(set.removeIf(subset::contains), !subset.isEmpty());
            assertTrue(set.isEmpty());

            assertEquals(set.addAll(subset), !subset.isEmpty());

            ModifiersSet copy = new ModifiersSet(set);
            assertEquals(copy.size(), set.size());
            assertTrue(copy.containsAll(set));
            assertTrue(set.containsAll(copy));
            assertEquals(copy, set);
            assertEquals(copy, subset);

            set.clear();
            assertEquals(copy, subset); //previous clear has no effect

            ModifiersSet collCopy = new ModifiersSet(subset);
            assertEquals(collCopy.size(), copy.size());
            assertTrue(collCopy.containsAll(copy));
            assertTrue(copy.containsAll(collCopy));
            assertEquals(collCopy, copy);
            assertEquals(collCopy, subset);
        }
    }

    @Test
    public void testEnforceUniqueCapabilities() {
        ModifiersSet set = new ModifiersSet();
        assertTrue(set.add(Projection.required("x")));
        assertTrue(set.add(SPARQLFilter.build("?u > 23")));
        assertTrue(set.add(Projection.required("y")));
        assertFalse(set.add(Projection.required("y")));

        assertEquals(set.size(), 2);
        assertEquals(set, Sets.newHashSet(Projection.required("y"),
                                          SPARQLFilter.build("?u > 23")));
    }

    @Test
    public void testNotifyAddAndRemove() {
        AtomicInteger adds = new AtomicInteger(), removes = new AtomicInteger();
        ModifiersSet set = new ModifiersSet();
        set.addListener(new ModifiersSet.Listener() {
            @Override public void added(@Nonnull Modifier modifier) {
                adds.incrementAndGet();
            }
            @Override public void removed(@Nonnull Modifier modifier) {
                removes.incrementAndGet();
            }
        });

        set.add(Projection.advised("x"));
        assertEquals(adds.get(), 1);
        assertEquals(removes.get(), 0);

        // operation with no effect: do not notify
        assertFalse(set.remove(Limit.advised(23)));
        assertEquals(adds.get(), 1);
        assertEquals(removes.get(), 0);

        // operation with no effect: do not notify
        assertFalse(set.add(Projection.advised("x")));
        assertEquals(adds.get(), 1);
        assertEquals(removes.get(), 0);

        // modifying copy does not notify source
        ModifiersSet copy = new ModifiersSet(set);
        assertEquals(copy, set);
        copy.add(Ask.ADVISED);
        assertEquals(adds.get(), 1);
        assertEquals(removes.get(), 0);

        // modifying source does not notify copy
        AtomicInteger copyAdds = new AtomicInteger(), copyRemoves = new AtomicInteger();
        copy.addListener(new ModifiersSet.Listener() {
            @Override public void added(@Nonnull Modifier modifier) {
                copyAdds.incrementAndGet();
            }
            @Override public void removed(@Nonnull Modifier modifier) {
                copyRemoves.incrementAndGet();
            }
        });
        assertTrue(set.remove(Projection.advised("x")));
        assertEquals(adds.get(), 1);
        assertEquals(removes.get(), 1); //set's listener is notified
        assertEquals(copyAdds.get(), 0);
        assertEquals(copyRemoves.get(), 0);

        // test add/remove of non-unique Modifier
        assertTrue(copy.add(SPARQLFilter.build("?u > 23")));
        assertTrue(copy.add(SPARQLFilter.build("?u > 24")));
        assertEquals(copyAdds.get(), 2);
        assertEquals(copyRemoves.get(), 0);
        assertTrue(copy.remove(SPARQLFilter.build("?u > 24")));
        assertEquals(copyAdds.get(), 2);
        assertEquals(copyRemoves.get(), 1);

        // changes in copy do not cause notifications in set
        assertEquals(adds.get(), 1);
        assertEquals(removes.get(), 1);

        // view does not notify changes in source
        AtomicInteger viewAdds = new AtomicInteger(), viewRemoves = new AtomicInteger();
        ModifiersSet view = set.getLockedView();
        view.addListener(new ModifiersSet.Listener() {
            @Override public void added(@Nonnull Modifier modifier) {
                viewAdds.incrementAndGet();
            }
            @Override public void removed(@Nonnull Modifier modifier) {
                viewRemoves.incrementAndGet();
            }
        });
        assertTrue(set.add(SPARQLFilter.build("?x < 23")));
        assertTrue(set.remove(SPARQLFilter.build("?x < 23")));
        assertEquals(adds.get(), 2);
        assertEquals(removes.get(), 2);
        assertEquals(viewAdds.get(), 0);
        assertEquals(viewRemoves.get(), 0);
    }

    @Test
    public void testReplaceLimitNotification() {
        List<Modifier> added = new ArrayList<>(), removed = new ArrayList<>();
        ModifiersSet set = new ModifiersSet();
        set.addListener(new ModifiersSet.Listener() {
            @Override public void added(@Nonnull Modifier modifier) {
                added.add(modifier);
            }
            @Override public void removed(@Nonnull Modifier modifier) {
                removed.add(modifier);
            }
        });

        assertTrue(set.add(Limit.advised(23)));
        assertFalse(set.add(Limit.advised(23)));
        assertTrue(set.add(Limit.advised(5)));

        assertEquals(added, asList(Limit.advised(23), Limit.advised(5)));
        assertEquals(removed, emptyList());
    }

    @Test
    public void testNotifyIteratorRemove() {
        List<Modifier> added = new ArrayList<>(), removed = new ArrayList<>();
        List<Modifier> otherAdded = new ArrayList<>(), otherRemoved = new ArrayList<>();

        ModifiersSet set = new ModifiersSet();
        ModifiersSet view1 = set.getLockedView();
        set.addListener(new ModifiersSet.Listener() {
            @Override public void added(@Nonnull Modifier modifier) {
                added.add(modifier);
            }

            @Override public void removed(@Nonnull Modifier modifier) {
                removed.add(modifier);
            }
        });
        ModifiersSet view2 = set.getLockedView();
        ModifiersSet copy1 = new ModifiersSet(set);
        assertTrue(set.add(Projection.advised("x")));
        assertTrue(set.add(Limit.advised(23)));
        ModifiersSet copy2 = new ModifiersSet(set);

        ModifiersSet.Listener otherListener = new ModifiersSet.Listener() {
            @Override public void added(@Nonnull Modifier modifier) {
                otherAdded.add(modifier);
            }
            @Override public void removed(@Nonnull Modifier modifier) {
                otherRemoved.add(modifier);
            }
        };

        view1.addListener(otherListener);
        view2.addListener(otherListener);
        copy1.addListener(otherListener);
        copy2.addListener(otherListener);

        Iterator<Modifier> it = set.iterator();
        Modifier victim1 = it.next();
        it.remove();
        assertTrue(it.hasNext());
        Modifier victim2 = it.next();
        it.remove();
        assertFalse(it.hasNext());

        assertEquals(removed, asList(victim1, victim2));
        assertEquals(added, asList(Projection.advised("x"), Limit.advised(23)));
        assertEquals(otherAdded, emptyList());
        assertEquals(otherRemoved, emptyList());
    }
}