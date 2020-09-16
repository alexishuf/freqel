package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class ListIdentityMultimapTest {
    @Test
    public void testSingleKey() {
        ListIdentityMultimap<RefSetTest.Thing, Integer> mm = new ListIdentityMultimap<>();
        RefSetTest.Thing zero = new RefSetTest.Thing(0);
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