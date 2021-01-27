package br.ufsc.lapesd.freqel.rel.mappings.r2rml;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.tags.ValueTag;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.RRFactory;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TriplesMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.UnsupportedRRException;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.impl.TriplesMapContext;
import br.ufsc.lapesd.freqel.rel.sql.JDBCCQEndpoint;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RiotException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

public class RRMappingW3CTest implements TestContext {
    private static final Logger logger = LoggerFactory.getLogger(RRMappingW3CTest.class);

    private static class TestCase implements Comparable<TestCase> {
        public final @Nonnull String dir, sqlFilename, rrFilename, rdfFilename;

        public TestCase(@Nonnull String dir, @Nonnull String sqlFilename,
                        @Nonnull String rrFilename, @Nonnull String rdfFilename) {
            this.dir = dir;
            this.sqlFilename = sqlFilename;
            this.rrFilename = rrFilename;
            this.rdfFilename = rdfFilename;
        }

        public @Nonnull String getSql() throws IOException {
            String path = extractedDir + "/" + dir + "/" + sqlFilename;
            try (FileInputStream in = new FileInputStream(path)) {
                return IOUtils.toString(in, StandardCharsets.UTF_8);
            }
        }

        private @Nonnull Model readModel(@Nonnull String path) throws IOException {
            try (FileInputStream in = new FileInputStream(path)) {
                Model model = ModelFactory.createDefaultModel();
                RDFDataMgr.read(model, in, Lang.TTL);
                return model;
            }
        }

        public @Nonnull Model getRR() throws IOException {
            return readModel(extractedDir + "/" + dir + "/" + rrFilename);
        }

        public @Nonnull File getRRFile() {
            return new File(extractedDir + "/" + dir + "/" + rrFilename);
        }

        public @Nonnull Model getRDF() throws IOException {
            return readModel(extractedDir + "/" + dir + "/" + rdfFilename);
        }

        public @Nonnull SetMultimap<String, String> getTableColumns() throws IOException {
            SetMultimap<String, String> mm = HashMultimap.create();
            StmtIterator stmtIt = getRR().listStatements(null, RR.subjectMap, (RDFNode) null);
            while (stmtIt.hasNext()) {
                TriplesMap tm = stmtIt.next().getSubject().as(TriplesMap.class);
                TriplesMapContext ctx = new TriplesMapContext(tm);
                String table = ctx.getTable();
                for (String column : ctx.getColumnNames())
                    mm.put(table, column);
            }
            return mm;
        }

        @Override
        public String toString() {
            return String.format("{%s, %s, %s, %s}", dir, rrFilename, rdfFilename, sqlFilename);
        }

        @Override
        public int compareTo(@NotNull RRMappingW3CTest.TestCase o) {
            return toString().compareTo(o.toString());
        }
    }

    private static File extractedDir;
    private static final Lock lock = new ReentrantLock();

    private static void extractTestCases() throws IOException {
        lock.lock();
        try {
            File outDir = Files.createTempDirectory("freqel-rdb2rdf-ts").toFile();
            outDir.deleteOnExit();
            try (InputStream in = RRMappingW3CTest.class.getResourceAsStream("rdb2rdf-ts.zip");
                 ZipInputStream zip = new ZipInputStream(in)) {
                for (ZipEntry e = zip.getNextEntry(); e != null; e = zip.getNextEntry()) {
                    if (e.isDirectory()) continue;
                    File outFile = new File(outDir + "/" + e.getName());
                    File parent = outFile.getParentFile();
                    if (!parent.exists() && !parent.mkdirs())
                        throw new IOException("Could not mkdirs for " + parent);
                    try (FileOutputStream out = new FileOutputStream(outFile)) {
                        IOUtils.copy(zip, out);
                    }
                }
                extractedDir = outDir;
            } catch (Throwable t) {
                try {
                    FileUtils.deleteDirectory(outDir);
                } catch (IOException e) {
                    t.addSuppressed(e);
                }
                throw t;
            }
        } finally {
            lock.unlock();
        }
    }

    @BeforeClass
    public void setUp() throws IOException {
        RRFactory.install();
        extractTestCases();
    }

    @AfterClass
    public void tearDown() throws IOException {
        lock.lock();
        try {
            if (extractedDir != null) {
                FileUtils.deleteDirectory(extractedDir);
                extractedDir = null;
            }
        } finally {
            lock.unlock();
        }
    }

    public static @Nonnull List<TestCase> getTestCases(int from, int to,
                                                       String... blacklistArray) throws IOException {
        BitSet bs = new BitSet();
        bs.set(from, to);
        Set<String> blacklist = new HashSet<>();
        for (String spec : blacklistArray) {
            blacklist.add(spec);
            if (spec.matches("\\d+"))
                bs.clear(Integer.parseInt(spec));
        }
        File[] dirs = extractedDir.listFiles(File::isDirectory);
        Pattern rx = Pattern.compile("^D(\\d+)-");
        List<TestCase> list = new ArrayList<>();
        for (File dir : dirs) {
            Matcher matcher = rx.matcher(dir.getName());
            if (!matcher.find()) {
                logger.warn("Ignoring unexpected directory {}", dir.getName());
                continue;
            }
            int dirIndex = Integer.parseInt(matcher.group(1));
            if (!bs.get(dirIndex)) continue;
            File sql = new File(dir, "create.sql");
            assert sql.exists();
            File[] mappings = dir.listFiles(n -> n.getName().matches("r2rml.?\\.ttl"));
            assertNotNull(mappings);
            for (File mapping : mappings) {
                String suffix = mapping.getName().replaceAll("r2rml(.?)\\.ttl", "$1");
                if (blacklist.contains(dirIndex + "/" + suffix))
                    continue;
                File mapped = new File(dir, "mapped" + suffix + ".nq");
                if (!mapped.exists())
                    continue; //mapping has errors
                Model model = ModelFactory.createDefaultModel();
                try (FileInputStream in = new FileInputStream(mapped)) {
                    RDFDataMgr.read(model, in, Lang.TTL);
                } catch (RiotException e) {
                    assert e.getMessage().contains("Triples not terminated by DOT")
                            : "Parse failed, but it may not be because of quads";
                    continue;
                }
                TestCase testCase = new TestCase(dir.getName(), sql.getName(),
                        mapping.getName(), mapped.getName());
                try {
                    testCase.getRR();
                    testCase.getTableColumns();
                } catch (UnsupportedRRException e) {
                    continue; //not a test case
                }
                list.add(testCase);
            }
        }
        list.sort(Comparator.naturalOrder());
        return list;
    }

    @DataProvider
    public static Object[][] toRDFData() throws IOException {
        extractTestCases();
        return getTestCases(0, 25,
                // deduplication of blank nodes  through templates is not enforced
                "5/b", "12/a", "12/b",
                "12/e", // [default mappings](https://www.w3.org/TR/r2rml/#default-mappings)
                        // are not supported
                "18/a" // JDBC (or H2) is removing trailing spaces, not our problem
        ).stream().map(c -> new Object[]{c}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "toRDFData", groups = {"fast"})
    public void testToRDF(@Nonnull TestCase testCase) throws Exception {
        SetMultimap<String, String> table2col = testCase.getTableColumns();

        RRMapping mapping = RRMapping.builder().strict(true).baseURI("http://example.com/base/")
                                               .load(testCase.getRRFile());
        Model rdf = ModelFactory.createDefaultModel();

        String url = "jdbc:h2:mem:"+ UUID.randomUUID().toString();
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(testCase.getSql());
            }

            for (String table : table2col.keySet()) {
                String sql = "SELECT " + String.join(", ", table2col.get(table)) +
                             " FROM " + table + ";";
                try (Statement stmt = connection.createStatement()) {
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        Map<String, Object> col2val = new HashMap<>();
                        for (String col : table2col.get(table)) {
                            String unquoted = col.replaceAll("^\"", "").replaceAll("\"$", "");
                            col2val.put(col, rs.getObject(unquoted));
                        }
                        mapping.toRDF(rdf, table, col2val);
                    }
                }
            }
        }

        Model expectedRDF = testCase.getRDF();
        if (!rdf.isIsomorphicWith(expectedRDF)) {
            System.out.println("vvvvvvvvvvvvvvvvvv  actual  vvvvvvvvvvvvvvvvvv");
            RDFDataMgr.write(System.out, rdf, RDFFormat.TURTLE_PRETTY);
            System.out.println("vvvvvvvvvvvvvvvvvv expected vvvvvvvvvvvvvvvvvv");
            RDFDataMgr.write(System.out, expectedRDF, RDFFormat.TURTLE_PRETTY);
        }
        assertTrue(rdf.isIsomorphicWith(expectedRDF));
    }

    private static @Nonnull List<CQuery> queriesFromClass(@Nonnull TestCase testCase) {
        try {
            RRMapping mapping = RRMapping.builder().load(testCase.getRR());
            Molecule mol = mapping.createMolecule();
            return mol.getIndex().stream(null, type, null)
                    .flatMap(t -> t.getObjAtom().getTags().stream())
                    .filter(ValueTag.class::isInstance)
                    .map(t -> ((ValueTag) t).getValue())
                    .filter(Term::isURI)
                    .map(t -> createQuery(x, type, t)).collect(toList());
        } catch (IOException e) {
            throw new AssertionError("Failed to load test case", e);
        }
    }

    private static @Nonnull List<CQuery> queriesFromPredicates(@Nonnull TestCase testCase) {
        try {
            List<CQuery> queries = new ArrayList<>();
            testCase.getRDF().listSubjects().forEachRemaining(subj -> {
                MutableCQuery query = new MutableCQuery();
                int[] count = {0};
                subj.listProperties().forEachRemaining(stmt -> {
                    StdVar obj = new StdVar("o" + (++count[0]));
                    query.add(new Triple(s, fromJena(stmt.getPredicate()), obj));
                });
                queries.add(query);
            });
            return queries;
        } catch (IOException e) {
            throw new AssertionError("Failed to load test case", e);
        }
    }

    private static @Nonnull List<CQuery> queriesFromSubject(@Nonnull TestCase testCase) {
        try {
            Model model = testCase.getRDF();
            List<CQuery> queries = new ArrayList<>();
            model.listSubjects().forEachRemaining(s -> {
                if (s.isURIResource())
                    queries.add(createQuery(fromJena(s), p, o));
            });
            return queries;
        } catch (IOException e) {
            throw new AssertionError("Failed to load test case", e);
        }
    }

    private static @Nonnull List<CQuery> queriesFromObjects(@Nonnull TestCase testCase) {
        try {
            List<CQuery> queries = new ArrayList<>();
            testCase.getRDF().listSubjects().forEachRemaining(subj -> {
                MutableCQuery query = new MutableCQuery();
                subj.listProperties().forEachRemaining(s -> {
                    if (!s.getObject().isAnon())
                        query.add(new Triple(x, fromJena(s.getPredicate()), fromJena(s.getObject())));
                });
                queries.add(query);
            });
            return queries;
        } catch (IOException e) {
            throw new AssertionError("Failed to load test case", e);
        }
    }

    @DataProvider
    public static @Nonnull Object[][] queryData() throws IOException {
        extractTestCases();
        return getTestCases(0, 25,
                // deduplication of blank nodes  through templates is not enforced
                "5/b", "12/a", "12/b",
                "11/b", // non-exclusive triple maps
                "12/e", // [default mappings](https://www.w3.org/TR/r2rml/#default-mappings) not supported
                "18/a" // JDBC (or H2) is removing trailing spaces, not our problem
        ).stream().flatMap(tc -> Stream.of(
                        queriesFromClass(tc).stream().map(q -> new Object[] {tc, q}),
                        queriesFromPredicates(tc).stream().map(q -> new Object[] {tc, q}),
                        queriesFromObjects(tc).stream().map(q -> new Object[] {tc, q})
                    ).flatMap(Function.identity())
                ).toArray(Object[][]::new);
    }

    private @Nonnull Set<Solution> wipeBlankNodes(@Nonnull Set<Solution> solutions) {
        Set<Solution> cleaned = new HashSet<>();
        for (Solution solution : solutions) {
            MapSolution.Builder builder = MapSolution.builder(solution);
            solution.forEach((name, term) -> {
                if (term.isBlank())
                    builder.remove(name);
            });
            cleaned.add(builder.build());
        }
        return cleaned;
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull TestCase testCase, @Nonnull CQuery query) throws Exception {
        RRMapping mapping = RRMapping.builder().strict(true).baseURI("http://example.com/base/")
                .load(testCase.getRRFile());
        String url = "jdbc:h2:mem:"+ UUID.randomUUID().toString();
        ARQEndpoint arqEp = ARQEndpoint.forModel(testCase.getRDF());
        JDBCCQEndpoint ep = JDBCCQEndpoint.createFor(mapping).connectingTo(url);
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(testCase.getSql()); //populate relational DB
            }

            Set<Solution> actual = new HashSet<>(), expected = new HashSet<>();
            ep.query(query).forEachRemainingThenClose(actual::add);
            arqEp.query(query).forEachRemainingThenClose(expected::add);
            assertEquals(actual.size(), expected.size());
            //consider any two blank nodes as equal
            actual   = wipeBlankNodes(actual);
            expected = wipeBlankNodes(expected);
            assertEquals(actual, expected);
        }
    }
}