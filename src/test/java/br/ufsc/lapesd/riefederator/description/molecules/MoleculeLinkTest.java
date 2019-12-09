package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Collections.emptySet;
import static org.testng.Assert.*;

public class MoleculeLinkTest {
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static final @Nonnull Atom a1 = new Atom("a", false, emptySet(), emptySet());
    public static final @Nonnull Atom a2 = new Atom("a", false, emptySet(), emptySet());
    public static final @Nonnull Atom b  = new Atom("b", false, emptySet(), emptySet());

    @DataProvider
    public static @Nonnull Object[][] equalsData() {
        return new Object[][] {
                new Object[] {new MoleculeLink(KNOWS, a1, true),
                              new MoleculeLink(KNOWS, a2, true),
                              true},
                new Object[] {new MoleculeLink(KNOWS, a1, true),
                              new MoleculeLink(KNOWS, a2, false),
                              false},
                new Object[] {new MoleculeLink(KNOWS, a1, true),
                              new MoleculeLink(KNOWS, b, true),
                              false},
                new Object[] {new MoleculeLink(KNOWS, a1, true),
                              new MoleculeLink(AGE,   a1, true),
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
        assertTrue( new MoleculeLink(KNOWS, a1, true ).isAuthoritative());
        assertFalse(new MoleculeLink(KNOWS, a1, false).isAuthoritative());
    }

}