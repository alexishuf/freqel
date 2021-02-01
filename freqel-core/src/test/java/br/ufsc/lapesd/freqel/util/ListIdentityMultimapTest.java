package br.ufsc.lapesd.freqel.util;

import br.ufsc.lapesd.freqel.util.ref.ListIdentityMultimap;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class ListIdentityMultimapTest {
    public static class Thing {
        private final int id;

        public Thing(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Thing)) return false;
            Thing thing = (Thing) o;
            return id == thing.id;
        }

        @Override public int hashCode() {
            return Objects.hash(id);
        }

        @Override public @Nonnull String toString() {
            return String.format("Thing(%d)", id);
        }
    }

    @Test
    public void testSingleKey() {
        ListIdentityMultimap<Thing, Integer> mm = new ListIdentityMultimap<>();
        Thing zero = new Thing(0);
        assertTrue(mm.putValue(zero, 0));
        assertTrue(mm.putValue(zero, 1));
        assertTrue(mm.putValue(zero, 0));
        assertEquals(mm.get(zero), Arrays.asList(0, 1, 0));

        assertTrue(mm.removeValue(zero, 0));
        assertEquals(mm.get(zero), Arrays.asList(1, 0));

        assertTrue(mm.removeValue(zero, 1));
        assertEquals(mm.get(zero), Collections.singletonList(0));

        assertTrue(mm.removeValue(zero, 0));
        assertNull(mm.get(zero));
    }
}