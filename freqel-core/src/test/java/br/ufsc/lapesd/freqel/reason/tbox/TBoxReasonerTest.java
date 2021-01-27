package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.NamedSupplier;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.reason.tbox.vlog.SystemVLogReasoner;
import com.github.lapesd.rdfit.RIt;
import com.github.lapesd.rdfit.iterator.RDFIt;
import com.github.lapesd.rdfit.source.RDFResource;
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
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TBoxReasonerTest implements TestContext {
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

    public static final List<NamedSupplier<TBoxReasoner>> suppliers = Arrays.asList(
            new NamedSupplier<>("HermiT", OWLAPITBoxReasoner::hermit),
            new NamedSupplier<>("StructuralReasoner", OWLAPITBoxReasoner::structural),
            new NamedSupplier<>("JFact", OWLAPITBoxReasoner::jFact),
            new NamedSupplier<>(TransitiveClosureTBoxReasoner.class),
            new NamedSupplier<>("SystemVLogReasoner", () -> {
                checkHasVLog();
                SystemVLogReasoner reasoner = new SystemVLogReasoner();
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
    private TBoxReasoner tBoxReasoner;

    @BeforeMethod(groups = {"fast"})
    public void setUp() {
        onto4 = new TBoxSpec().addResource(getClass(), "../../onto-4.ttl");
        onto5 = new TBoxSpec().addResource(getClass(), "../../onto-5.ttl");
    }

    @AfterMethod(groups = {"fast"})
    public void tearDown() throws Exception {
        TBoxReasoner local = this.tBoxReasoner;
        tBoxReasoner = null;
        if (local != null) local.close();
        for (File dir = tempFiles.poll(); dir != null; dir = tempFiles.poll())
            FileUtils.forceDelete(dir);
    }

    @DataProvider
    public static Object[][] supplierData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
    public void testLoadDoesNotBlowUp(@Nonnull Supplier<TBoxReasoner> supplier) {
        supplier.get().load(onto5);
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
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

    @Test(dataProvider = "supplierData", groups = {"fast"})
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

    @Test(dataProvider = "supplierData", invocationCount = 64, threadPoolSize = 1)
    public void testLoadOverwritesRepeat(Supplier<TBoxReasoner> supplier) {
        tBoxReasoner = supplier.get();
        tBoxReasoner.load(onto5);
        assertTrue(tBoxReasoner.subClasses(d).collect(toSet()).contains(d1));
        assertFalse(tBoxReasoner.subClasses(c).collect(toSet()).contains(c1));

        tBoxReasoner.load(onto4);
        assertFalse(tBoxReasoner.subClasses(d).collect(toSet()).contains(d1));
        assertTrue(tBoxReasoner.subClasses(c).collect(toSet()).contains(c1));
        assertTrue(tBoxReasoner.subClasses(c).collect(toSet()).contains(c2));
    }

    @Test(dataProvider = "supplierData", groups = {"fast"})
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


    @Test(dataProvider = "supplierData", groups = {"fast"})
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

    @Test(dataProvider = "supplierData", groups = {"fast"})
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


    @Test(dataProvider = "supplierData")
    public void testLargeRDFBenchTBox(NamedSupplier<TBoxReasoner> supplier) throws Exception {
        if (supplier.getName().equals("HermiT"))
            return; // HermiT will refuse some axioms
        tBoxReasoner = supplier.get();

        File tboxFile = hdtFileWithoutImports("../../LargeRDFBench-tbox.hdt");
        TBoxSpec spec = new TBoxSpec().add(tboxFile);
        tBoxReasoner.load(spec);

        StdURI scientist    = new StdURI(DBO + "Scientist");
        StdURI person       = new StdURI(DBO + "Person");
        StdURI entomologist = new StdURI(DBO + "Entomologist");
        Set<Term> set = tBoxReasoner.subClasses(scientist).collect(toSet());
        assertTrue(set.contains(entomologist));
        assertFalse(set.contains(person));
        assertFalse(set.contains(scientist));

        set = tBoxReasoner.subClasses(person).collect(toSet());
        assertTrue(set.contains(scientist));
        assertTrue(set.contains(entomologist));
        assertFalse(set.contains(person));

        StdURI isClassifiedBy = new StdURI(DUL + "isClassifiedBy");
        StdURI genre          = new StdURI(DBO + "genre");
        StdURI literaryGenre  = new StdURI(DBO + "literaryGenre");
        set = tBoxReasoner.subProperties(genre).collect(toSet());
        assertTrue(set.contains(literaryGenre));
        assertFalse(set.contains(genre));
        assertFalse(set.contains(isClassifiedBy));

        set = tBoxReasoner.subProperties(isClassifiedBy).collect(toSet());
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