package br.ufsc.lapesd.riefederator.model.term.factory;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.jena.model.term.JenaTermFactory;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdTermFactory;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import com.google.common.collect.Lists;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

public class TermFactoryTest {
    public static List<NamedFunction<TermFactory, TermFactory>> wrappers =
            asList(new NamedFunction<>("SynchronizedTermFactory", SynchronizedTermFactory::new),
                   new NamedFunction<>("CachedTermFactory", CachedTermFactory::new));
    public static List<NamedSupplier<? extends TermFactory>> nativeSuppliers =
            asList(new NamedSupplier<>(StdTermFactory.class),
                   new NamedSupplier<>(JenaTermFactory.class));
    public static List<Supplier<? extends TermFactory>> suppliers;
    public static List<Term> stdTerms = asList(
            new StdURI("https://example.org/Alice"),
            StdLit.fromEscaped("1", new StdURI(XSDDatatype.XSDint.getURI())),
            StdLit.fromUnescaped("1", new StdURI(XSDDatatype.XSDint.getURI())),
            StdLit.fromUnescaped("bacon", "en"),
            StdLit.fromUnescaped("ba\tcon", "en"),
            StdLit.fromEscaped("ba\\tcon", "en"),
            new StdVar("x")
    );

    static {
        suppliers = new ArrayList<>();
        suppliers.addAll(nativeSuppliers);
        for (Function<TermFactory, TermFactory> wrapper : wrappers) {
            for (NamedSupplier<? extends TermFactory> supplier : nativeSuppliers)
                suppliers.add(new NamedSupplier<>(
                        String.format("%s(%s)", wrapper, supplier),
                        () -> wrapper.apply(supplier.get())
                ));
        }
    }

    private static void assertEquals(Object actual, Object expected) {
        Assert.assertEquals(actual, expected);
        if (actual instanceof Term && actual.equals(expected))
            Assert.assertEquals(actual.hashCode(), expected.hashCode());
    }

    @DataProvider
    public static Object[][] factoriesData() {
        return suppliers.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] factoriesAndTermsData() {
        return Lists.cartesianProduct(suppliers, stdTerms).stream().map(List::toArray)
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "factoriesData")
    public void testCreateBlank(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        Blank b1 = f.createBlank();
        Blank b2 = f.createBlank();
        Blank b3 = supplier.get().createBlank();
        assertEquals(b1, b1);
        assertNotEquals(b1, b2);
        assertNotEquals(b1, b3);
    }

    @Test(dataProvider = "factoriesData")
    public void testURIEquality(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        URI a = f.createURI("http://example.org/ns#1");
        URI b = f.createURI("http://example.org/ns#2");
        URI c = f.createURI("http://example.org/ns#1");
        URI d = f.createURI("https://example.org/ns#1");

        assertEquals(a, c);
        assertNotEquals(a, b);
        assertNotEquals(d, a);
    }

    @Test(dataProvider = "factoriesData")
    public void testURIString(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        URI u = f.createURI("http://example.org/ns#1");
        assertEquals(u.getURI(), "http://example.org/ns#" + "1");
    }

    @Test(dataProvider = "factoriesData")
    public void testPlainLiteral(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        Lit a = f.createLit("asd");
        Lit b = f.createLit("qwe");
        Lit c = f.createLit("asd");

        assertEquals(a.getLexicalForm(), "asd");
        assertEquals(b.getDatatype().getURI(), XSDDatatype.XSDstring.getURI());
        assertNull(c.getLangTag());

        assertEquals(a, c);
        assertNotEquals(a, b);
    }


    @Test(dataProvider = "factoriesData")
    public void testInt(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        Lit a = f.createLit("1", XSDDatatype.XSDint.getURI());
        Lit b = f.createLit("2", XSDDatatype.XSDint.getURI());
        Lit c = f.createLit("1", XSDDatatype.XSDstring.getURI());
        Lit d = f.createLit("1", XSDDatatype.XSDint.getURI());

        assertEquals(a.getLexicalForm(), "1");
        assertEquals(a.getLexicalForm(), c.getLexicalForm());
        assertEquals(a.getLexicalForm(), d.getLexicalForm());

        assertEquals(a.getDatatype().getURI(), XSDDatatype.XSDint.getURI());
        assertEquals(c.getDatatype().getURI(), XSDDatatype.XSDstring.getURI());
        assertNull(a.getLangTag());

        assertEquals(a, d);
        assertNotEquals(a, b);
        assertNotEquals(a, c);
    }

    @Test(dataProvider = "factoriesData")
    public void testEscapedString(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        Lit a = f.createLit("1\\\"", XSDDatatype.XSDstring.getURI(), true);
        Lit b = f.createLit("1\"", XSDDatatype.XSDstring.getURI(), false);
        Lit c = f.createLit("1\"", XSDDatatype.XSDstring.getURI());
        Lit d = f.createLit("2\\\"", XSDDatatype.XSDstring.getURI(), true);

        assertEquals(a.getLexicalForm(), "1\"");
        assertEquals(b.getLexicalForm(), "1\"");
        assertEquals(c.getLexicalForm(), "1\"");
        assertEquals(d.getLexicalForm(), "2\"");

        assertEquals(a.getDatatype().getURI(), XSDDatatype.XSDstring.getURI());
        assertEquals(b.getDatatype().getURI(), XSDDatatype.XSDstring.getURI());
        assertEquals(c.getDatatype().getURI(), XSDDatatype.XSDstring.getURI());
        assertEquals(d.getDatatype().getURI(), XSDDatatype.XSDstring.getURI());

        assertEquals(a, b);
        assertEquals(a, c);
        assertNotEquals(a, d);
    }

    @Test(dataProvider = "factoriesData")
    public void testLangLiteral(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        Lit a = f.createLangLit("café", "fr");
        Lit b = f.createLangLit("café", "pt");
        Lit c = f.createLangLit("café", "fr");

        assertEquals(a.getLangTag(), "fr");
        assertEquals(b.getLangTag(), "pt");
        assertEquals(a.getLexicalForm(), b.getLexicalForm());
        assertEquals(a, c);
        assertNotEquals(a, b);
    }

    @Test(dataProvider = "factoriesData")
    public void testEscapedLangLiteral(Supplier<TermFactory> supplier) {
        TermFactory f = supplier.get();
        Lit a = f.createLangLit("\\tcafé\\\"", "fr", true);
        Lit b = f.createLangLit("café", "pt");
        Lit c = f.createLangLit("\tcafé\"", "fr", false);
        Lit d = f.createLangLit("\tcafé\"", "fr");

        assertEquals(a.getLangTag(), "fr");
        assertEquals(b.getLangTag(), "pt");
        assertEquals(c.getLangTag(), "fr");
        assertEquals(a.getLexicalForm(), c.getLexicalForm());
        assertEquals(a.getLexicalForm(), d.getLexicalForm());
        assertNotEquals(a, b);
        assertEquals(a, c);
        assertEquals(a, d);
    }

    @Test(dataProvider = "factoriesAndTermsData")
    public void testEqualsWithStdTerm(Supplier<TermFactory> supplier, Term std) {
        TermFactory f = supplier.get();
        System.out.printf("%s, %s\n", f, std);
        Term instance = null;
        if (std.isURI()) {
            instance =f.createURI(std.asURI().getURI());
        } else if (std.isVar()) {
            try {
                instance = f.createVar(std.asVar().getName());
            } catch (UnsupportedOperationException ignored) {
                // factory does not support createVar()
                return;
            }
        } else if (std.isLiteral()) {
            Lit lit = std.asLiteral();
            String tag = lit.getLangTag();
            if (tag != null) {
                instance = f.createLangLit(lit.getLexicalForm(), tag);
            } else {
                instance = f.createLit(lit.getLexicalForm(), lit.getDatatype());
            }
        }
        if (instance == null)
            throw new SkipException("Could not create instance from"+std);
        assertEquals(instance, std);
    }

}