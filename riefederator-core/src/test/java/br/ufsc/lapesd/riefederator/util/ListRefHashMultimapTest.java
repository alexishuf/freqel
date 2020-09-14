package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class ListRefHashMultimapTest {
    @Test
    public void testSingleKey() {
        ListRefHashMultimap<RefMapTest.Thing, Integer> mm = new ListRefHashMultimap<>();
        RefMapTest.Thing zero = new RefMapTest.Thing(0);
        assertTrue(mm.putValue(zero, 0));
        assertTrue(mm.putValue(zero, 1));
        assertTrue(mm.putValue(zero, 0));
        assertEquals(mm.get(zero), Arrays.asList(0, 1, 0));

        assertTrue(mm.removeValue(zero, 0));
        assertEquals(mm.get(zero), Arrays.asList(1, 0));

        assertTrue(mm.removeValue(zero, 1));
        assertEquals(mm.get(zero), Arrays.asList(0));

        assertTrue(mm.removeValue(zero, 0));
        assertNull(mm.get(zero));
    }
}