package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class AtomTest {
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static final @Nonnull Atom a1 = new Atom("a", false, emptySet(), emptySet());
    public static final @Nonnull Atom a2 = new Atom("a", false, emptySet(), emptySet());
    public static final @Nonnull Atom b  = new Atom("b", false, emptySet(), emptySet());

    @DataProvider
    public static Object[][] equalsData() {
        Set<MoleculeLink> s1 = singleton(new MoleculeLink(KNOWS, a1, false));
        Set<MoleculeLink> s2 = singleton(new MoleculeLink(KNOWS, a2, false));
        Set<MoleculeLink> s3 = singleton(new MoleculeLink(AGE,   a1, false));
        return new Object[][] {
                new Object[] {new Atom("atom1", false, emptySet(), emptySet()),
                              new Atom("atom1", false, emptySet(), emptySet()), true},
                new Object[] {new Atom("atom1", true, emptySet(), emptySet()),
                              new Atom("atom1", true, emptySet(), emptySet()), true},
                new Object[] {new Atom("atom1", false, emptySet(), emptySet()),
                              new Atom("atom2", false, emptySet(), emptySet()), false},
                new Object[] {new Atom("atom1", true,  emptySet(), emptySet()),
                              new Atom("atom1", false, emptySet(), emptySet()), false},
                new Object[] {new Atom("atom1", true, s1, emptySet()),
                              new Atom("atom1", true, s2, emptySet()), true},
                new Object[] {new Atom("atom1", true, s1, emptySet()),
                              new Atom("atom1", true, s3, emptySet()), false},
                new Object[] {new Atom("atom1", true, emptySet(), s1),
                              new Atom("atom1", true, emptySet(), s2), true},
                new Object[] {new Atom("atom1", true, emptySet(), s1),
                              new Atom("atom1", true, emptySet(), s3), false},
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
        Atom a = new Atom("atom23", true,
                          singleton(new MoleculeLink(KNOWS, a1, false)),
                          singleton(new MoleculeLink(KNOWS, b , true )));
        assertTrue(a.isExclusive());
        assertEquals(a.getIn(), singleton(new MoleculeLink(KNOWS, a1, false)));
        assertEquals(a.getOut(), singleton(new MoleculeLink(KNOWS, b, true )));
    }

}