package br.ufsc.lapesd.freqel.description.molecules;

import br.ufsc.lapesd.freqel.TestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Collections.emptySet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class MoleculeLinkTest implements TestContext {
    public static final @Nonnull Atom a1 = new Atom("a");
    public static final @Nonnull Atom a2 = new Atom("a");
    public static final @Nonnull Atom b  = new Atom("b");

    @DataProvider
    public static @Nonnull Object[][] equalsData() {
        return new Object[][] {
                new Object[] {new MoleculeLink(knows, a1, true, emptySet()),
                              new MoleculeLink(knows, a2, true, emptySet()),
                              true},
                new Object[] {new MoleculeLink(knows, a1, true, emptySet()),
                              new MoleculeLink(knows, a2, false, emptySet()),
                              false},
                new Object[] {new MoleculeLink(knows, a1, true, emptySet()),
                              new MoleculeLink(knows, b, true, emptySet()),
                              false},
                new Object[] {new MoleculeLink(knows, a1, true, emptySet()),
                              new MoleculeLink(age,   a1, true, emptySet()),
                              false},
        };
    }

    @Test(dataProvider = "equalsData")
    public void testEqual(MoleculeLink left, MoleculeLink right, boolean expected) {
        if (expected) {
            assertEquals(left, right);
            assertEquals(left.hashCode(), right.hashCode());
        } else {
            assertNotEquals(left, right);
        }
    }

    @Test
    public void testAuthoritative() {
        assertTrue( new MoleculeLink(knows, a1, true, emptySet()).isAuthoritative());
        assertFalse(new MoleculeLink(knows, a1, false, emptySet()).isAuthoritative());
    }

}