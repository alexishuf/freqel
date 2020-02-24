package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.TestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class AtomTest implements TestContext {
    public static final @Nonnull Atom a1 = new Atom("a", false, false, false, emptySet(), emptySet());
    public static final @Nonnull Atom a2 = new Atom("a", false, false, false, emptySet(), emptySet());
    public static final @Nonnull Atom b  = new Atom("b", false, false, false, emptySet(), emptySet());

    @DataProvider
    public static Object[][] equalsData() {
        Set<MoleculeLink> s1 = singleton(new MoleculeLink(knows, a1, false));
        Set<MoleculeLink> s2 = singleton(new MoleculeLink(knows, a2, false));
        Set<MoleculeLink> s3 = singleton(new MoleculeLink(age,   a1, false));
        return new Object[][] {
                new Object[] {new Atom("atom1", false, false, false, emptySet(), emptySet()),
                              new Atom("atom1", false, false, false, emptySet(), emptySet()), true},
                new Object[] {new Atom("atom1", true, false, false, emptySet(), emptySet()),
                              new Atom("atom1", true, false, false, emptySet(), emptySet()), true},
                new Object[] {new Atom("atom1", false, false, false, emptySet(), emptySet()),
                              new Atom("atom2", false, false, false, emptySet(), emptySet()), false},
                new Object[] {new Atom("atom1", true,  false, false, emptySet(), emptySet()),
                              new Atom("atom1", false, false, false, emptySet(), emptySet()), false},
                new Object[] {new Atom("atom1", false,  false, false, emptySet(), emptySet()),
                              new Atom("atom1", false, true, false, emptySet(), emptySet()), false},
                new Object[] {new Atom("atom1", false,  false, false, emptySet(), emptySet()),
                              new Atom("atom1", false, false, true, emptySet(), emptySet()), false},
                new Object[] {new Atom("atom1", true, false, false, s1, emptySet()),
                              new Atom("atom1", true, false, false, s2, emptySet()), true},
                new Object[] {new Atom("atom1", true, false, false, s1, emptySet()),
                              new Atom("atom1", true, false, false, s3, emptySet()), false},
                new Object[] {new Atom("atom1", true, false, false, emptySet(), s1),
                              new Atom("atom1", true, false, false, emptySet(), s2), true},
                new Object[] {new Atom("atom1", true, false, false, emptySet(), s1),
                              new Atom("atom1", true, false, false, emptySet(), s3), false},
        };
    }


    @Test(dataProvider = "equalsData")
    public void testEquals(Atom left, Atom right, boolean expected) {
        if (expected) {
            assertEquals(left, right);
            assertEquals(left.hashCode(), right.hashCode());
        } else {
            assertNotEquals(left, right);
        }
    }

    @Test
    public void testGetters() {
        Atom a = new Atom("atom23", true, true,
                false, singleton(new MoleculeLink(knows, a1, false)),
                          singleton(new MoleculeLink(knows, b , true )));
        assertTrue(a.isExclusive());
        assertTrue(a.isClosed());
        assertEquals(a.getIn(), singleton(new MoleculeLink(knows, a1, false)));
        assertEquals(a.getOut(), singleton(new MoleculeLink(knows, b, true )));

        Atom b = new Atom("atom23", true, false, false, emptySet(), emptySet());
        assertTrue(b.isExclusive());
        assertFalse(b.isClosed());
    }

}