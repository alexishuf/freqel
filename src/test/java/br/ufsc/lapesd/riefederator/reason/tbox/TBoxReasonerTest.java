package br.ufsc.lapesd.riefederator.reason.tbox;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TBoxReasonerTest {
    public static final List<NamedSupplier<TBoxReasoner>> suppliers = Arrays.asList(
            new NamedSupplier<>("HermiT", OWLAPITBoxReasoner::hermit),
            new NamedSupplier<>("StructuralReasoner", OWLAPITBoxReasoner::structural),
            new NamedSupplier<>("JFact", OWLAPITBoxReasoner::jFact),
            new NamedSupplier<>(TransitiveClosureTBoxReasoner.class)
    );

    private static final StdURI c = new StdURI("http://example.org/onto-4.ttl#C");
    private static final StdURI c1 = new StdURI("http://example.org/onto-4.ttl#C1");
    private static final StdURI c2 = new StdURI("http://example.org/onto-4.ttl#C2");

    private static final StdURI d = new StdURI("http://example.org/onto-5.ttl#D");
    private static final StdURI d1 = new StdURI("http://example.org/onto-5.ttl#D1");
    private static final StdURI d11 = new StdURI("http://example.org/onto-5.ttl#D11");
    private static final StdURI d111 = new StdURI("http://example.org/onto-5.ttl#D111");
    private static final StdURI d1111 = new StdURI("http://example.org/onto-5.ttl#D1111");
    private static final StdURI d12 = new StdURI("http://example.org/onto-5.ttl#D12");
    private static final StdURI d2 = new StdURI("http://example.org/onto-5.ttl#D2");

    private static final StdURI p = new StdURI("http://example.org/onto-5.ttl#p");
    private static final StdURI p1 = new StdURI("http://example.org/onto-5.ttl#p1");
    private static final StdURI p11 = new StdURI("http://example.org/onto-5.ttl#p11");
    private static final StdURI p111 = new StdURI("http://example.org/onto-5.ttl#p111");
    private static final StdURI p1111 = new StdURI("http://example.org/onto-5.ttl#p1111");
    private static final StdURI p12 = new StdURI("http://example.org/onto-5.ttl#p12");
    private static final StdURI p2 = new StdURI("http://example.org/onto-5.ttl#p2");

    private TBoxSpec onto4, onto5;
    private TBoxReasoner tBoxReasoner;

    @BeforeMethod
    public void setUp() {
        onto4 = new TBoxSpec().addResource(getClass(), "../../onto-4.ttl");
        onto5 = new TBoxSpec().addResource(getClass(), "../../onto-5.ttl");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        TBoxReasoner local = this.tBoxReasoner;
        tBoxReasoner = null;
        if (local != null) local.close();
    }

    @DataProvider
    public static Object[][] supplierData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "supplierData")
    public void testLoadDoesNotBlowUp(@Nonnull Supplier<TBoxReasoner> supplier) {
        supplier.get().load(onto5);
    }

    @Test(dataProvider = "supplierData")
    public void testDirectSubclass(Supplier<TBoxReasoner> supplier) {
        tBoxReasoner = supplier.get();
        tBoxReasoner.load(onto5);
        Set<Term> set = tBoxReasoner.subClasses(d).collect(toSet());
        assertTrue(set.contains(d1));
        assertFalse(set.contains(d));

        set = tBoxReasoner.subClasses(d1).collect(toSet());
        assertTrue(set.contains(d11));
        assertFalse(set.contains(d1));
        assertFalse(set.contains(d2));
    }

    @Test(dataProvider = "supplierData")
    public void testLoadOverwrites(Supplier<TBoxReasoner> supplier) {
        tBoxReasoner = supplier.get();
        tBoxReasoner.load(onto5);
        assertTrue(tBoxReasoner.subClasses(d).collect(toSet()).contains(d1));
        assertFalse(tBoxReasoner.subClasses(c).collect(toSet()).contains(c1));

        tBoxReasoner.load(onto4);
        assertFalse(tBoxReasoner.subClasses(d).collect(toSet()).contains(d1));
        assertTrue(tBoxReasoner.subClasses(c).collect(toSet()).contains(c1));
        assertTrue(tBoxReasoner.subClasses(c).collect(toSet()).contains(c2));
    }

    @Test(dataProvider = "supplierData")
    public void testIndirectSubclass(NamedSupplier<TBoxReasoner> supplier) {
        if (supplier.getName().equals("StructuralReasoner"))
            return; // mock reasoner, no transitivity
        tBoxReasoner = supplier.get();
        tBoxReasoner.load(onto5);

        Set<Term> set = tBoxReasoner.subClasses(d).collect(toSet());
        assertTrue(set.contains(d1));
        assertTrue(set.contains(d2));
        assertTrue(set.contains(d11));
        assertTrue(set.contains(d12));
        assertTrue(set.contains(d111));
        assertTrue(set.contains(d1111));

        set = tBoxReasoner.subClasses(d1).collect(toSet());
        assertFalse(set.contains(d1));
        assertFalse(set.contains(d2));
        assertTrue(set.contains(d11));
        assertTrue(set.contains(d12));
        assertTrue(set.contains(d111));
        assertTrue(set.contains(d1111));
    }


    @Test(dataProvider = "supplierData")
    public void testDirectSubProperty(Supplier<TBoxReasoner> supplier) {
        tBoxReasoner = supplier.get();
        tBoxReasoner.load(onto5);

        Set<Term> set = tBoxReasoner.subProperties(p).collect(toSet());
        assertFalse(set.contains(p));
        assertTrue(set.contains(p1));
        assertTrue(set.contains(p2));

        set = tBoxReasoner.subProperties(p1).collect(toSet());
        assertFalse(set.contains(p1));
        assertFalse(set.contains(p2));
        assertTrue(set.contains(p11));
        assertTrue(set.contains(p12));
    }

    @Test(dataProvider = "supplierData")
    public void testIndirectSubProperty(NamedSupplier<TBoxReasoner> supplier) {
        if (supplier.getName().equals("StructuralReasoner"))
            return; // mock reasoner, no transitivity
        tBoxReasoner = supplier.get();
        tBoxReasoner.load(onto5);

        Set<Term> set = tBoxReasoner.subProperties(p).collect(toSet());
        assertFalse(set.contains(p));
        assertTrue(set.contains(p1));
        assertTrue(set.contains(p11));
        assertTrue(set.contains(p111));
        assertTrue(set.contains(p1111));
        assertTrue(set.contains(p12));
        assertTrue(set.contains(p2));

        set = tBoxReasoner.subProperties(p1).collect(toSet());
        assertFalse(set.contains(p));
        assertFalse(set.contains(p1));
        assertTrue(set.contains(p11));
        assertTrue(set.contains(p111));
        assertTrue(set.contains(p1111));
        assertTrue(set.contains(p12));
        assertFalse(set.contains(p2));
    }
}