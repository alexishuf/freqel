package br.ufsc.lapesd.freqel.description.molecules.annotations;

import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class AtomAnnotationTest {
    private static final Atom BOOK = new Atom("Book");
    private static final Atom AUTHOR = new Atom("Author");
    private static final AtomAnnotation BOOK_AN_1 = AtomAnnotation.of(BOOK);
    private static final AtomAnnotation BOOK_AN_2 = AtomAnnotation.of(BOOK);
    private static final AtomAnnotation AUTHOR_AN = AtomAnnotation.of(AUTHOR);
    private static final AtomInputAnnotation R_AUTHOR_AN_1 = AtomInputAnnotation.asRequired(AUTHOR, "author").get();
    private static final AtomInputAnnotation R_AUTHOR_AN_2 = AtomInputAnnotation.asRequired(AUTHOR, "author").get();
    private static final AtomInputAnnotation O_AUTHOR_AN_1 = AtomInputAnnotation.asOptional(AUTHOR, "author").get();
    private static final AtomInputAnnotation O_AUTHOR_AN_2 = AtomInputAnnotation.asOptional(AUTHOR, "author").get();


    @Test
    public void testInputAnnotationDifferentFromPlain() {
        assertNotEquals(R_AUTHOR_AN_1, AUTHOR_AN);
        assertNotEquals(O_AUTHOR_AN_1, AUTHOR_AN);
    }

    @Test
    public void testPlainEquals() {
        assertEquals(BOOK_AN_1, BOOK_AN_2);
        assertEquals(BOOK_AN_1, BOOK_AN_1);
        assertNotEquals(BOOK_AN_1, AUTHOR_AN);
    }

    @Test
    public void testInputEquals() {
        assertEquals(R_AUTHOR_AN_1, R_AUTHOR_AN_1);
        assertEquals(R_AUTHOR_AN_1, R_AUTHOR_AN_2);
        assertEquals(O_AUTHOR_AN_1, O_AUTHOR_AN_1);
        assertEquals(O_AUTHOR_AN_1, O_AUTHOR_AN_2);
        assertNotEquals(R_AUTHOR_AN_1, O_AUTHOR_AN_1);
    }

    @Test
    public void testIsRequired() {
        assertTrue(R_AUTHOR_AN_1.isRequired());
        assertFalse(O_AUTHOR_AN_1.isRequired());
    }

    @Test
    public void testIsInput() {
        assertFalse(BOOK_AN_1 instanceof AtomInputAnnotation);
        assertTrue(R_AUTHOR_AN_1.isInput());
        assertTrue(O_AUTHOR_AN_1.isInput());
    }


}