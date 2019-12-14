package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class MoleculeTest {
    public static final @Nonnull Atom a1 = new Atom("a");
    public static final @Nonnull Atom a2 = new Atom("a");
    public static final @Nonnull Atom b  = new Atom("b");
    public static final @Nonnull Atom c  = new Atom("c");

    @DataProvider
    public static Object[][] equalsData() {
        return new Object[][] {
                new Object[] {new Molecule(a1), new Molecule(a2), true},
                new Object[] {new Molecule(a1), new Molecule(b), false},
        };
    }

    @Test(dataProvider = "equalsData")
    public void testEquals(Molecule left, Molecule right, boolean expected) {
        if (expected) {
            assertEquals(left, right);
            assertEquals(left.hashCode(), right.hashCode());
        } else {
            assertNotEquals(left, right);
        }
    }

}