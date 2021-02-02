package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.owlapi.reason.tbox.OWLAPITBoxMaterializer;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.parse.CQueryContext;
import br.ufsc.lapesd.freqel.util.NamedSupplier;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.reason.tbox.vlog.SystemVLogMaterializer;
import com.github.lapesd.rdfit.RIt;
import com.github.lapesd.rdfit.iterator.RDFIt;
import com.github.lapesd.rdfit.source.RDFResource;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jena.vocabulary.OWL2;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.TripleWriter;
import org.rdfhdt.hdt.triples.TripleString;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class TBoxMaterializerTest implements TestContext {
    private static final Queue<File> tempFiles = new LinkedBlockingQueue<>();
    private static Boolean hasVLog = null;
    private static void checkHasVLog() {
        if (hasVLog != null) {
            if (!hasVLog)
                throw new SkipException("VLog not available");
            return;
        }
        try {
            Process process = new ProcessBuilder("vlog", "help")
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .start();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                hasVLog = false;
                throw new SkipException("Slow vlog help, will skip tests");
            }
        } catch (InterruptedException|IOException e) {
            hasVLog = false;
            throw new SkipException("Could not run VLog", e);
        }
    }

    public static final List<NamedSupplier<TBoxMaterializer>> suppliers = Arrays.asList(
            new NamedSupplier<>("HermiT", OWLAPITBoxMaterializer::hermit),
            new NamedSupplier<>("StructuralReasoner", OWLAPITBoxMaterializer::structural),
            new NamedSupplier<>("JFact", OWLAPITBoxMaterializer::jFact),
            new NamedSupplier<>(TransitiveClosureTBoxMaterializer.class),
            new NamedSupplier<>("SystemVLogReasoner", () -> {
                checkHasVLog();
                SystemVLogMaterializer reasoner = new SystemVLogMaterializer();
                try {
                    File parent = Files.createTempDirectory("freqel").toFile();
                    tempFiles.add(parent);
                    reasoner.setVlogTempDirParent(parent);
                } catch (IOException e) {
                    throw new AssertionError("Failed to create temp dir", e);
                }
                return reasoner;
            })
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
    private TBoxMaterializer tBoxMaterializer;

    @BeforeMethod(groups = {"fast"})
    public void setUp() {
        onto4 = new TBoxSpec().addResource(getClass(), "../../onto-4.ttl");
        onto5 = new TBoxSpec().addResource(getClass(), "../../onto-5.ttl");
    }

    @AfterMethod(groups = {"fast"})
    public void tearDown() throws Exception {
        TBoxMaterializer local = this.tBoxMaterializer;
        tBoxMaterializer = null;
        if (local != null) local.close();
        for (File dir = tempFiles.poll(); dir != null; dir = tempFiles.poll())
            FileUtils.forceDelete(dir);
    }

    @DataProvider
    public static Object[][] supplierData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testLoadDoesNotBlowUp(@Nonnull Supplier<TBoxMaterializer> supplier) {
        supplier.get().load(onto5);
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testDirectSubclass(Supplier<TBoxMaterializer> supplier) {
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);
        Set<Term> set = tBoxMaterializer.subClasses(d).collect(toSet());
        assertTrue(set.contains(d1));
        assertFalse(set.contains(d));

        Set<Term> withSet = tBoxMaterializer.withSubClasses(d).collect(toSet());
        assertTrue(withSet.containsAll(set));
        assertTrue(withSet.contains(d));
        assertEquals(withSet.size(), set.size()+1);

        set = tBoxMaterializer.subClasses(d1).collect(toSet());
        assertTrue(set.contains(d11));
        assertFalse(set.contains(d1));
        assertFalse(set.contains(d2));

        withSet = tBoxMaterializer.withSubClasses(d1).collect(toSet());
        assertTrue(withSet.containsAll(set));
        assertTrue(withSet.contains(d1));
        assertEquals(withSet.size(), set.size()+1);
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testSubClassOfEndpoint(Supplier<TBoxMaterializer> supplier) {
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);
        TPEndpoint ep = tBoxMaterializer.getEndpoint();
        if (ep == null)
            return; //nothing to test

        Set<Term> set = new HashSet<>();
        ep.query(createQuery(x, subClassOf, d2)).forEachRemainingThenClose(s -> set.add(s.get(x)));
        assertEquals(set, newHashSet(d2));

        set.clear();
        ep.query(createQuery(x, subClassOf, d111)).forEachRemainingThenClose(s -> set.add(s.get(x)));
        assertEquals(set, newHashSet(d111, d1111));

        set.clear();
        ep.query(createQuery(x, subClassOf, d)).forEachRemainingThenClose(s -> set.add(s.get(x)));
        assertEquals(set, newHashSet(d, d1, d11, d111, d1111, d12, d2));
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testSubPropertyOfEndpoint(Supplier<TBoxMaterializer> supplier) {
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);
        TPEndpoint ep = tBoxMaterializer.getEndpoint();
        if (ep == null)
            return; //nothing to test

        Set<Term> set = new HashSet<>();
        ep.query(createQuery(x, subPropertyOf, p2)).forEachRemainingThenClose(s -> set.add(s.get(x)));
        assertEquals(set, newHashSet(p2));

        set.clear();
        ep.query(createQuery(x, subPropertyOf, p111)).forEachRemainingThenClose(s -> set.add(s.get(x)));
        assertEquals(set, newHashSet(p111, p1111));

        set.clear();
        ep.query(createQuery(x, subPropertyOf, p)).forEachRemainingThenClose(s -> set.add(s.get(x)));
        assertEquals(set, newHashSet(p, p1, p11, p111, p1111, p12, p2));
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testLoadOverwrites(Supplier<TBoxMaterializer> supplier) {
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);
        assertTrue(tBoxMaterializer.subClasses(d).collect(toSet()).contains(d1));
        assertFalse(tBoxMaterializer.subClasses(c).collect(toSet()).contains(c1));

        tBoxMaterializer.load(onto4);
        assertFalse(tBoxMaterializer.subClasses(d).collect(toSet()).contains(d1));
        assertTrue(tBoxMaterializer.subClasses(c).collect(toSet()).contains(c1));
        assertTrue(tBoxMaterializer.subClasses(c).collect(toSet()).contains(c2));
    }

    @Test(dataProvider = "supplierData", invocationCount = 64, threadPoolSize = 1)
    public void testLoadOverwritesRepeat(Supplier<TBoxMaterializer> supplier) {
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);
        assertTrue(tBoxMaterializer.subClasses(d).collect(toSet()).contains(d1));
        assertFalse(tBoxMaterializer.subClasses(c).collect(toSet()).contains(c1));

        tBoxMaterializer.load(onto4);
        assertFalse(tBoxMaterializer.subClasses(d).collect(toSet()).contains(d1));
        assertTrue(tBoxMaterializer.subClasses(c).collect(toSet()).contains(c1));
        assertTrue(tBoxMaterializer.subClasses(c).collect(toSet()).contains(c2));
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testIndirectSubclass(NamedSupplier<TBoxMaterializer> supplier) {
        if (supplier.getName().equals("StructuralReasoner"))
            return; // mock reasoner, no transitivity
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);

        Set<Term> set = tBoxMaterializer.subClasses(d).collect(toSet());
        assertTrue(set.contains(d1));
        assertTrue(set.contains(d2));
        assertTrue(set.contains(d11));
        assertTrue(set.contains(d12));
        assertTrue(set.contains(d111));
        assertTrue(set.contains(d1111));

        Set<Term> withSet = tBoxMaterializer.withSubClasses(d).collect(toSet());
        assertTrue(withSet.contains(d));
        assertTrue(withSet.containsAll(set));
        assertEquals(withSet.size(), set.size()+1);

        set = tBoxMaterializer.subClasses(d1).collect(toSet());
        assertFalse(set.contains(d1));
        assertFalse(set.contains(d2));
        assertTrue(set.contains(d11));
        assertTrue(set.contains(d12));
        assertTrue(set.contains(d111));
        assertTrue(set.contains(d1111));

        withSet = tBoxMaterializer.withSubClasses(d1).collect(toSet());
        assertTrue(withSet.contains(d1));
        assertTrue(withSet.containsAll(set));
        assertEquals(withSet.size(), set.size()+1);
    }


    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testDirectSubProperty(Supplier<TBoxMaterializer> supplier) {
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);

        Set<Term> set = tBoxMaterializer.subProperties(p).collect(toSet());
        assertFalse(set.contains(p));
        assertTrue(set.contains(p1));
        assertTrue(set.contains(p2));

        set = tBoxMaterializer.subProperties(p1).collect(toSet());
        assertFalse(set.contains(p1));
        assertFalse(set.contains(p2));
        assertTrue(set.contains(p11));
        assertTrue(set.contains(p12));
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testIndirectSubProperty(NamedSupplier<TBoxMaterializer> supplier) {
        if (supplier.getName().equals("StructuralReasoner"))
            return; // mock reasoner, no transitivity
        tBoxMaterializer = supplier.get();
        tBoxMaterializer.load(onto5);

        Set<Term> set = tBoxMaterializer.subProperties(p).collect(toSet());
        assertFalse(set.contains(p));
        assertTrue(set.contains(p1));
        assertTrue(set.contains(p11));
        assertTrue(set.contains(p111));
        assertTrue(set.contains(p1111));
        assertTrue(set.contains(p12));
        assertTrue(set.contains(p2));

        Set<Term> withSet = tBoxMaterializer.withSubProperties(p).collect(toSet());
        assertTrue(withSet.containsAll(set));
        assertTrue(withSet.contains(p));
        assertEquals(withSet.size(), set.size()+1);

        set = tBoxMaterializer.subProperties(p1).collect(toSet());
        assertFalse(set.contains(p));
        assertFalse(set.contains(p1));
        assertTrue(set.contains(p11));
        assertTrue(set.contains(p111));
        assertTrue(set.contains(p1111));
        assertTrue(set.contains(p12));
        assertFalse(set.contains(p2));

        withSet = tBoxMaterializer.withSubProperties(p1).collect(toSet());
        assertTrue(withSet.containsAll(set));
        assertTrue(withSet.contains(p1));
        assertEquals(withSet.size(), set.size()+1);
    }


    @Test(dataProvider = "supplierData")
    public void testLargeRDFBenchTBox(NamedSupplier<TBoxMaterializer> supplier) throws Exception {
        if (supplier.getName().equals("HermiT"))
            return; // HermiT will refuse some axioms
        tBoxMaterializer = supplier.get();

        File tboxFile = hdtFileWithoutImports("../../LargeRDFBench-tbox.hdt");
        TBoxSpec spec = new TBoxSpec().add(tboxFile);
        tBoxMaterializer.load(spec);

        StdURI scientist    = new StdURI(DBO + "Scientist");
        StdURI person       = new StdURI(DBO + "Person");
        StdURI entomologist = new StdURI(DBO + "Entomologist");
        Set<Term> set = tBoxMaterializer.subClasses(scientist).collect(toSet());
        assertTrue(set.contains(entomologist));
        assertFalse(set.contains(person));
        assertFalse(set.contains(scientist));

        set = tBoxMaterializer.subClasses(person).collect(toSet());
        assertTrue(set.contains(scientist));
        assertTrue(set.contains(entomologist));
        assertFalse(set.contains(person));

        StdURI isClassifiedBy = new StdURI(DUL + "isClassifiedBy");
        StdURI genre          = new StdURI(DBO + "genre");
        StdURI literaryGenre  = new StdURI(DBO + "literaryGenre");
        set = tBoxMaterializer.subProperties(genre).collect(toSet());
        assertTrue(set.contains(literaryGenre));
        assertFalse(set.contains(genre));
        assertFalse(set.contains(isClassifiedBy));

        set = tBoxMaterializer.subProperties(isClassifiedBy).collect(toSet());
        assertTrue(set.contains(genre));
        assertTrue(set.contains(literaryGenre));
        assertFalse(set.contains(isClassifiedBy));
    }

    private @Nonnull File hdtFileWithoutImports(String resourcePath) throws Exception {
        File hdtFile = Files.createTempFile("freqel", ".hdt").toFile();
        hdtFile.deleteOnExit();
        tempFiles.add(hdtFile);

        try (FileOutputStream out = new FileOutputStream(hdtFile);
             RDFResource resource = new RDFResource(getClass(), resourcePath);
             RDFIt<TripleString> it = RIt.iterateTriples(TripleString.class, resource);
             TripleWriter writer = HDTManager.getHDTWriter(out, hdtFile.toURI().toString(),
                                                           new HDTSpecification())) {
            while (it.hasNext()) {
                TripleString t = it.next();
                if (!t.getPredicate().toString().equals(OWL2.imports.getURI()))
                    writer.addTriple(t);
            }
        }
        return hdtFile;
    }
}