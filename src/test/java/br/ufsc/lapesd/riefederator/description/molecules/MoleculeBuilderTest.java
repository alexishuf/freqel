package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class MoleculeBuilderTest {
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static final @Nonnull Atom a1 = new Atom("a", false, emptySet(), emptySet());
    public static final @Nonnull Atom b  = new Atom("b", false, emptySet(), emptySet());
    public static final @Nonnull Atom c  = new Atom("c", false, emptySet(), emptySet());

    @Test
    public void testBuildAtomOnlyIn() {
        Atom core = new MoleculeBuilder("core").exclusive(true).in(KNOWS, a1).buildAtom();
        assertEquals(core.getName(), "core");
        assertTrue(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(KNOWS, a1, false)));
        assertEquals(core.getOut(), emptySet());
    }

    @Test
    public void testBuildAtomInOut() {
        Atom core = new MoleculeBuilder("core").exclusive(false).in(KNOWS, a1)
                .out(AGE, b).outAuthoritative(AGE, c).buildAtom();
        assertEquals(core.getName(), "core");
        assertFalse(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(KNOWS, a1, false)));
        assertEquals(core.getOut(), newHashSet(new MoleculeLink(AGE, b, false),
                                               new MoleculeLink(AGE, c, true)));
    }

    @Test
    public void testBuildAtomThenMolecule() {
        MoleculeBuilder builder = new MoleculeBuilder("core");
        Atom core = builder.exclusive(false).in(KNOWS, a1).out(AGE, b).buildAtom();
        Molecule molecule = builder.build();

        assertEquals(core.getName(), "core");
        assertFalse(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(KNOWS, a1, false)));
        assertEquals(core.getOut(), singleton(new MoleculeLink(AGE, b, false)));

        assertEquals(molecule.getCore(), core);
    }
}