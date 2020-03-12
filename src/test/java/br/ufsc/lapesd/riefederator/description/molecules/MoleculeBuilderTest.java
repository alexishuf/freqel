package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.TestContext;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class MoleculeBuilderTest implements TestContext {
    public static final @Nonnull Atom a1 = new Atom("a");
    public static final @Nonnull Atom b  = new Atom("b");
    public static final @Nonnull Atom c  = new Atom("c");

    @Test
    public void testBuildAtomOnlyIn() {
        Atom core = new MoleculeBuilder("core").exclusive(true).in(knows, a1).buildAtom();
        assertEquals(core.getName(), "core");
        assertTrue(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(knows, a1, false)));
        assertEquals(core.getOut(), emptySet());
    }

    @Test
    public void testBuildAtomInOut() {
        Atom core = new MoleculeBuilder("core").exclusive(false).in(knows, a1)
                .out(age, b).outAuthoritative(age, c).buildAtom();
        assertEquals(core.getName(), "core");
        assertFalse(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(knows, a1, false)));
        assertEquals(core.getOut(), newHashSet(new MoleculeLink(age, b, false),
                                               new MoleculeLink(age, c, true)));
    }

    @Test
    public void testBuildAtomThenMolecule() {
        MoleculeBuilder builder = new MoleculeBuilder("core");
        Atom core = builder.exclusive(false).in(knows, a1).out(age, b).buildAtom();
        Molecule molecule = builder.build();

        assertEquals(core.getName(), "core");
        assertFalse(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(knows, a1, false)));
        assertEquals(core.getOut(), singleton(new MoleculeLink(age, b, false)));

        assertEquals(molecule.getCore(), core);
    }
}