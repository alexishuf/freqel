package br.ufsc.lapesd.riefederator.testgen;

import br.ufsc.lapesd.riefederator.model.NTParseException;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdTermFactory;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLQueryParser;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.vocabulary.RDF;
import org.glassfish.jersey.client.ClientProperties;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.riefederator.model.RDFUtils.toNT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;

/**
 * This is not a test class. It downloads LargeRDFBench 1] queries and their results from [2]
 * and generates minimal set of .ttl files that allow executing all queries under the mediator
 *
 * [1]: https://github.com/dice-group/LargeRDFBench
 * [2]: http://goo.gl/8tX1Pa
 */
@SuppressWarnings("FieldMayBeFinal")
public class LargeRDFBenchTestResourcesGenerator {
    private static final Logger logger =
            LoggerFactory.getLogger(LargeRDFBenchTestResourcesGenerator.class);
    private static final String ZIP_URL =
            "https://drive.google.com/uc?export=download&id=0BzemFAUFXpqOU1R3XzQ0SzVUcFE";
    private static final Pattern URI_RX = Pattern.compile("^https?://|^urn:");
    private static final Pattern QUOTE_RX = Pattern.compile("^\"(.*)\"$");
    private static final String SK_NS = "https://example.org/sk#";
    private static final AtomicInteger skNextId = new AtomicInteger(1);

    @Option(name = "--help", aliases = {"-h"}, help = true, usage = "Shows this help")
    private boolean help;

    @Option(name = "--no-purge", usage = "Do NOT remove all previous contents of OUT_DIR")
    private boolean noPurgeOld = false;

    @Option(name = "--out-dir", required = true,
            usage = "Destination root for test resources. Will be created if does not exist")
    private File outDir;

    @Option(name = "--results-zip", forbids = {"--results-dir"},
            usage = "Path to BigRDFBench-Queries_Results.zip")
    private File resultsZip;

    private boolean deleteResultsZip = false;

    @Option(name = "--results-dir", forbids = {"--results-zip"},
            usage = "Path to extracted BigRDFBench-Queries_Results.zip")
    private File resultsDir;

    @Option(name = "--max-results-per-query", usage = "When a query has many results, only " +
            "take at most this number of results and ignore the remainder")
    private int maxResultsPerQuery = 10;

    @Option(name = "--distribution-yaml", required = true,
            usage = "Path to YAML file with distribution rules. If this file does not exist, " +
                    "it will be created and --dumps-dir is required")
    private File distYaml;

    @Option(name = "--dist-max-triples-file", usage = "Maximum number of triples to inspect " +
            "in each RDF file (a dataset may have multiple files)")
    private int distMaxTriplesPerFile = 50000;

    @Option(name = "--dist-max-secs-archive", usage = "Sets a maximum number of seconds to be " +
            "spent processing a single archive. After this time elapses no new file " +
            "extractions will occur. This time may be overrun, since always at least one " +
            "extraction and parsing will always take place.")
    private int distMaxSecsPerArchive = 120;

    private DistributionRules distributionRules;

    @Option(name = "--dumps-dir",
            usage = "Directory with dumps (one compressed archive per dataset)")
    private File dumpsDir;

    private enum Dataset {
        LINKED_TCGAM,
        LINKED_TCGAE,
        LINKED_TCGAA,
        CHEBI,
        DBPEDIA,
        DRUGBANK,
        GEONAMES,
        JAMENDO,
        KEGG,
        LINKEDMDB,
        NYT,
        SWDF,
        AFFYMETRIX;

        public @Nonnull String baseName() {
            switch (this) {
                case LINKED_TCGAM: return "LinkedTCGA-M";
                case LINKED_TCGAE: return "LinkedTCGA-E";
                case LINKED_TCGAA: return "LinkedTCGA-A";
                case CHEBI: return "ChEBI";
                case DBPEDIA: return "DBPedia-Subset";
                case DRUGBANK: return "DrugBank";
                case GEONAMES: return "GeoNames";
                case JAMENDO: return "Jamendo";
                case KEGG: return "KEGG";
                case LINKEDMDB: return "LMDB";
                case NYT: return "NYT";
                case SWDF: return "SWDFood";
                case AFFYMETRIX: return "Affymetrix";
                default: break;
            }
            throw new IllegalArgumentException();
        }

        public @Nullable File getDumpFile(@Nonnull File dir) {
            for (String ext : asList("7z", "zip", "rar")) {
                File file = new File(dir, baseName() + "." + ext);
                if (file.exists()) return file;
                file = new File(dir, baseName() + ext.toUpperCase());
                if (file.exists()) return file;
            }
            return null;
        }

        public @Nonnull File getNTFile(@Nonnull File outDir) {
            return new File(outDir, "data/" + baseName() + ".nt");
        }

        public @Nonnull Writer openNTFileWriter(@Nonnull File outDir) throws FileNotFoundException {
            File file = getNTFile(outDir);
            return new OutputStreamWriter(new FileOutputStream(file, true), UTF_8);
        }
    }

    public static void main(String[] args) throws Exception {
        LargeRDFBenchTestResourcesGenerator app = new LargeRDFBenchTestResourcesGenerator();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getLocalizedMessage());
            showHelp(parser, System.err);
            System.exit(1);
        }

        if (app.help) showHelp(parser, System.out);
        else          app.run();
    }

    private static Term result2Term(@Nonnull String value) {
        if (URI_RX.matcher(value).find())
            return fromJena(createResource(value));
        Matcher matcher = QUOTE_RX.matcher(value);
        if (matcher.matches())
            return StdLit.fromEscaped(matcher.group(1));
        try {
            return RDFUtils.fromNT(value, new StdTermFactory());
        } catch (NTParseException e) {
            return StdLit.fromEscaped(value);
        }
    }

    public static CollectionResults parseResults(@Nonnull String queryName,
                                             @Nonnull BufferedReader reader) throws IOException {
        return parseResults(queryName, reader, Integer.MAX_VALUE);
    }

    public static CollectionResults parseResults(@Nonnull String queryName,
                                                 @Nonnull BufferedReader reader,
                                                 int maxResults) throws IOException {
        Splitter splitter = Splitter.on("<===>");
        List<String> varNames;
        List<Solution> solutions;
        String line = reader.readLine();
        boolean removeAssignments = false;
        if (queryName.equals("B7") && line.equals("patient<===>p<===>o")) {
            line = "p<===>patient<===>o";
            removeAssignments = true;
        }
        varNames = splitter.splitToList(line);
        line = reader.readLine(); //header is not data
        solutions = new ArrayList<>();
        for (int i = 0; i < maxResults && line != null; line = reader.readLine(), ++i) {
            List<String> values = splitter.splitToList(line);
            MapSolution.Builder builder = MapSolution.builder();
            for (int j = 0; j < varNames.size(); j++) {
                String value = values.get(j);
                if (removeAssignments)
                    value = value.replaceAll("^\\w+=", "");
                builder.put(varNames.get(j), result2Term(value));
            }
            solutions.add(builder.build());
        }
        return new CollectionResults(solutions, varNames);
    }

    private void run() throws Exception {
        try {
            setupOutDir();
            setupDistributionYaml();
            if (resultsDir == null && resultsZip == null)
                resultsZip = downloadZip();
            if (resultsDir != null) {
                if (!resultsDir.exists())
                    throw new IOException("--results-dir " + resultsDir + " does not exist");
                if (!resultsDir.isDirectory())
                    throw new IOException("--results-dir " + resultsDir + " is not a directory");
            }
            if (resultsZip != null && !resultsZip.exists())
                throw new IOException("--results-zip " + resultsZip + " does not exist");

            for (Iterator<BenchmarkQuery> it = listPairs(); it.hasNext(); ) {
                BenchmarkQuery query = it.next();
                try {
                    query.parse();
                    addQuery(query);
                } catch (SPARQLParseException e) {
                    logger.warn("Query {} is not supported: {}", query.queryName, e.getMessage());
                }
            }
            File[] dataFiles = new File(outDir, "data").listFiles();
            assert dataFiles != null;
            for (File dataFile : dataFiles)
                removeDuplicates(dataFile);
        } finally {
            if (deleteResultsZip && resultsZip != null && !resultsZip.delete())
                logger.error("Failed to delete downloaded temp file {}", resultsZip);
        }
    }

    private void removeDuplicates(@Nonnull File file) throws IOException {
        LinkedHashSet<String> lines = new LinkedHashSet<>();
        int lineCount = 0;
        try (FileInputStream stream = new FileInputStream(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream, UTF_8))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                lines.add(line);
                ++lineCount;
            }
        }
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(file), UTF_8)) {
            for (String line : lines) {
                writer.write(line);
                writer.write("\n");
            }
        }
        if (lineCount != lines.size())
            logger.info("Removed {} duplicate triples from {}", lineCount - lines.size(), file);
    }

    private static final class DistributionObservations {
        private final SetMultimap<String, Dataset> subHost2ds = HashMultimap.create();
        private final SetMultimap<String, Dataset> pred2ds = HashMultimap.create();
        private final SetMultimap<String, Dataset> class2ds = HashMultimap.create();
        private final SetMultimap<String, Dataset> objHost2ds = HashMultimap.create();
        private final SetMultimap<String, Dataset> prefix2ds = HashMultimap.create();

        private static void put(@Nonnull Multimap<String, Dataset> map, @Nullable String key,
                                @Nonnull Dataset value) {
            if (key == null) return;
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (map) {
                map.put(key, value);
            }
        }

        public void putSubHost2ds(@Nullable String host, @Nonnull Dataset ds) {
            put(subHost2ds, host, ds);
        }
        public void putPred2ds(@Nullable String predicate, @Nonnull Dataset ds) {
            put(pred2ds, predicate, ds);
        }
        public void putObjHost2ds(@Nullable String host, @Nonnull Dataset ds) {
            put(objHost2ds, host, ds);
        }
        public void putClass2ds(@Nullable String classURI, @Nonnull Dataset ds) {
            put(class2ds, classURI, ds);
        }
        public void putPrefixHost2ds(@Nullable String host, @Nonnull Dataset ds) {
            put(prefix2ds, host, ds);
        }
    }

    private static final class DistributionRules {
        private final Map<String, Dataset> subHost2ds;
        private final Map<String, Dataset> pred2ds;
        private final Map<String, Dataset> objHost2ds;
        private final Map<String, Dataset> class2ds;
        private final Map<String, Dataset> prefix2ds;

        public DistributionRules() {
            subHost2ds = new HashMap<>();
            pred2ds = new HashMap<>();
            objHost2ds = new HashMap<>();
            class2ds = new HashMap<>();
            prefix2ds = new HashMap<>();
        }
        public DistributionRules(@Nonnull DictTree dictTree) {
            subHost2ds = toDatasetMap(dictTree.getMapNN("unique/subject/host2ds"));
            pred2ds = toDatasetMap(dictTree.getMapNN("unique/predicate/uri2ds"));
            objHost2ds = toDatasetMap(dictTree.getMapNN("unique/object/host2ds"));
            class2ds = toDatasetMap(dictTree.getMapNN("unique/class/uri2ds"));
            prefix2ds = toDatasetMap(dictTree.getMapNN("unique/prefix2ds"));
        }

        public DistributionRules(@Nonnull DistributionObservations observations) {
            this();
            fromMultimap(subHost2ds, observations.subHost2ds);
            fromMultimap(pred2ds, observations.pred2ds);
            fromMultimap(objHost2ds, observations.objHost2ds);
            fromMultimap(class2ds, observations.class2ds);
            fromMultimap(prefix2ds, observations.prefix2ds);
        }

        private void fromMultimap(Map<String, Dataset> dest, Multimap<String, Dataset> src) {
            for (String key : src.keys()) {
                if (src.get(key).size() == 1)
                    dest.put(key, src.get(key).iterator().next());
            }
        }

        private Map<String, Dataset> toDatasetMap(DictTree dict) {
            Map<String, Dataset> map = new HashMap<>();
            for (String key : dict.keySet()) {
                String escaped = key.replaceAll("/", "%2F");
                map.put(key, Dataset.valueOf(dict.getString(escaped)));
            }
            return map;
        }

        public DistributionRules(@Nonnull File yamlOrJsonFile) throws IOException {
            this(DictTree.load().fromFile(yamlOrJsonFile));
        }

        public void saveToYaml(@Nonnull File file) throws IOException {
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs())
                throw new IOException("Failed to mkdirs "+file.getParentFile());
            try (FileOutputStream stream = new FileOutputStream(file)) {
                saveToYaml(stream);
            }
        }
        public void saveToYaml(@Nonnull @WillClose OutputStream stream) throws IOException {
            try (OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8)) {
                saveToYaml(writer);
            }
        }
        public void saveToYaml(@Nonnull @WillClose Writer writer) {
            try (PrintWriter out = new PrintWriter(writer)) {
                out.printf("unique:\n");

                out.printf("  subject:\n");
                writeDatasetMap(out, "    ", "host2ds", subHost2ds);

                out.printf("  predicate:\n");
                writeDatasetMap(out, "    ", "uri2ds", pred2ds);

                out.printf("  object:\n");
                writeDatasetMap(out, "    ", "host2ds", objHost2ds);

                out.printf("  class:\n");
                writeDatasetMap(out, "    ", "uri2ds", class2ds);

                writeDatasetMap(out, "  ", "prefix2ds", prefix2ds);
            }
        }

        private void writeDatasetMap(@Nonnull PrintWriter out, @Nonnull String indent,
                                     @Nonnull String key,
                                     @Nonnull Map<String, Dataset> map) {
            out.printf("%s%s:\n", indent, key);
            indent += "  ";
            for (Map.Entry<String, Dataset> e : map.entrySet())
                out.printf("%s%s: %s\n", indent, e.getKey(), e.getValue().name());
        }

        public @Nullable Dataset getDatasetFor(Triple triple) {
            Dataset dataset = getHostUnique(triple.getSubject(), subHost2ds);
            if (dataset != null) return dataset;
            dataset = pred2ds.get(triple.getPredicate().asURI().getURI());
            if (dataset != null) return dataset;
            dataset = getHostUnique(triple.getObject(), objHost2ds);
            if (dataset != null) return dataset;

            dataset = getPrefixUnique(triple.getSubject());
            if (dataset != null) return dataset;
            dataset = getPrefixUnique(triple.getPredicate());
            if (dataset != null) return dataset;
            dataset = getPrefixUnique(triple.getObject());
            return dataset;
        }

        private @Nullable Dataset getPrefixUnique(Term term) {
            if (!term.isVar()) return null;

            String uri = term.asURI().getURI();
            for (Map.Entry<String, Dataset> e : prefix2ds.entrySet()) {
                if (uri.startsWith(e.getKey()))
                    return e.getValue();
            }
            return null;
        }

        private @Nullable Dataset getHostUnique(Term term, Map<String, Dataset> host2ds) {
            try {
                if (term.isURI()) {
                    String host = new URI(term.asURI().getURI()).getHost();
                    return host2ds.get(host);
                }
            } catch (URISyntaxException ignored) { }
            return null;
        }
    }

    private void setupDistributionYaml() throws IOException {
        if (distYaml.exists()) {
            if (!distYaml.isFile())
                throw new RuntimeException("--distribution-yaml "+distYaml+" exists but is not file!");
            distributionRules = new DistributionRules(distYaml);
            return;
        }
        if (!distYaml.exists() && dumpsDir == null) {
            throw new RuntimeException("--distribution-yaml "+
                                       distYaml+" does not exist and --dumps-dir not given!");
        }

        DistributionObservations observations = new DistributionObservations();
        List<String> missing = new ArrayList<>();
        for (Dataset ds : Dataset.values()) {
            if (ds.getDumpFile(dumpsDir) == null)
                missing.add(ds.baseName());
        }
        if (!missing.isEmpty())
            throw new RuntimeException("Missing dataset dumps on "+dumpsDir+": "+missing);
        stream(Dataset.values()).parallel().forEach(ds -> inspectDataset(ds, observations));
        distributionRules = new DistributionRules(observations);
        distributionRules.saveToYaml(distYaml);
    }

    private void inspectDataset(@Nonnull Dataset ds,
                                @Nonnull DistributionObservations observations) {
        File dumpFile = ds.getDumpFile(dumpsDir);
        assert dumpFile != null;
        logger.info("Processing {}...", ds);
        try {
            new SevenZipRDFStream(dumpFile)
                    .setTimeout(distMaxSecsPerArchive, TimeUnit.SECONDS)
                    .streamAll(new StreamRDFBase() {
                int triples = 0;
                @Override
                public void start() {
                    triples = 0;
                }

                @Override
                public void triple(org.apache.jena.graph.Triple triple) {
                    observations.putSubHost2ds(getHost(triple.getSubject()), ds);
                    observations.putPred2ds(triple.getPredicate().getURI(), ds);
                    observations.putObjHost2ds(getHost(triple.getObject()), ds);
                    if (triple.getPredicate().getURI().equals(RDF.type.getURI()))
                        observations.putClass2ds(triple.getObject().getURI(), ds);
                    ++triples;
                    if (triples > distMaxTriplesPerFile)
                        throw new SevenZipRDFStream.InterruptFileException();
                }
                @Override
                public void prefix(String prefix, String iri) {
                    observations.prefix2ds.put(iri, ds);
                }
            });
        } catch (IOException e) {
            logger.error("Exception while processing {} at {}", ds, dumpFile, e);
        }
        logger.info("Completed {}.", ds);
    }

    private @Nullable String getHost(@Nonnull Node node) {
        if (node.isURI()) {
            try {
                return new URI(node.getURI()).getHost();
            } catch (URISyntaxException ignored) { }
        }
        return null;
    }

    private void addQuery(BenchmarkQuery query) throws IOException {
        logger.info("Adding query {}...", query.queryName);
        extractFile(query.getQueryReader(), "queries/"+query.queryName);
        extractFile(query.getResultsReader(), "results/"+query.queryName,
                    maxResultsPerQuery+1);
        assert query.cQuery != null;
        Results results = query.parseResults();
        while (results.hasNext()) {
            Solution solution = results.next();
            switch (query.queryName) {
                case "B2":
                    // satisfy FILTER(?weight <= 55), since ?weight is not in results
                    solution = MapSolution.builder(solution)
                            .put("weight", fromJena(createTypedLiteral(55)))
                            .build();
                    break;
                case "B5":
                    solution = MapSolution.builder(solution)
                            .put("start", fromJena(createTypedLiteral(1)))
                            .put("position", fromJena(createTypedLiteral(2)))
                            .put("stop", fromJena(createTypedLiteral(3)))
                            .put("chromosome", fromJena(createTypedLiteral("X123")))
                            .put("lookupChromosome", fromJena(createTypedLiteral("X123")))
                            .build();
                    break;
                case "B6":
                    solution = MapSolution.builder(solution)
                            .put("lookupChromosome", fromJena(createTypedLiteral("X123")))
                            .put("chromosome", fromJena(createTypedLiteral("X123")))
                            .build();
                    break;
                case "B7":
                    solution = MapSolution.builder(solution)
                            .put("popDensity", fromJena(createTypedLiteral(32)))
                            .build();
                    break;
            }

            List<Triple> graph = rebuildGraph(query.cQuery, solution);
            storeTriples(graph);
        }
    }

    private void storeTriples(List<Triple> triples) throws IOException {
        Map<Triple, File> triple2file = new HashMap<>();
        File dataDir = new File(outDir, "data");
        for (Triple t : triples) {
            Dataset ds = distributionRules.getDatasetFor(t);
            String baseName = ds == null ? null : ds.baseName();
            if (ds == null) {
                String tcgaPrefix = "http://tcga.deri.ie";
                String predURI = t.getPredicate().asURI().getURI();
                String objURI = t.getObject().isURI() ? t.getObject().asURI().getURI() : "";
                String subjURI = t.getSubject().isURI() ? t.getSubject().asURI().getURI() : "";
                boolean isTCGA = predURI.startsWith(tcgaPrefix)
                        || subjURI.startsWith(tcgaPrefix)
                        || (predURI.equals(RDF.type.getURI()) && objURI.startsWith(tcgaPrefix));
                boolean isGeoNames = subjURI.startsWith("http://sws.geonames.org")
                        || predURI.startsWith("http://www.geonames.org/ontology");
                if (isTCGA)
                    baseName = "tcga-orphan";
                else if (predURI.startsWith("http://dbpedia.org/ontology"))
                    baseName = Dataset.DBPEDIA.baseName();
                else if (isGeoNames)
                    baseName = Dataset.GEONAMES.baseName();
                else if (predURI.equals(RDF.type.getURI()) && objURI.startsWith("http://bio2rdf.org/ns/kegg"))
                    baseName = Dataset.KEGG.baseName();
                else if (predURI.startsWith("http://www4.wiwiss.fu-berlin.de/drugbank/"))
                    baseName = Dataset.DRUGBANK.baseName();
            }
            if (baseName != null)
                triple2file.put(t, new File(dataDir, baseName + ".nt"));
        }
        for (Triple triple : triples) {
            File file = triple2file.get(triple);
            if (file == null) { // use same file where the subject has been saved in another triple
                for (Map.Entry<Triple, File> e : triple2file.entrySet()) {
                    if (e.getKey().getSubject().equals(triple.getSubject()))
                        file = e.getValue();
                }
            }
            if (file == null) // last resort
                file = new File(dataDir, "orphan.nt");
            try (FileOutputStream stream = new FileOutputStream(file, true);
                 PrintStream out = new PrintStream(stream)) {
                out.printf("%s %s %s .\n", toNT(triple.getSubject()), toNT(triple.getPredicate()),
                                           toNT(triple.getObject()));
            }
        }

    }

    private static final class Skolemizer {
        private @Nonnull Solution solution;
        private @Nonnull Map<Term, Term> skolemized = new HashMap<>();

        public Skolemizer(@Nonnull Solution solution) {
            this.solution = solution;
        }

        public Term getTerm(@Nonnull Term term) {
            if (!term.isVar()) return term;
            Term bound = solution.get(term.asVar());
            if (bound != null) return bound;
            return skolemized.
                    computeIfAbsent(term, t -> new StdURI(SK_NS + skNextId.getAndIncrement()));
        }
    }

    private List<Triple> rebuildGraph(CQuery cQuery, Solution solution) {
        List<Triple> list = new ArrayList<>(cQuery.size());
        Skolemizer skolemizer = new Skolemizer(solution);
        for (Triple triple : cQuery) {
            Term s = skolemizer.getTerm(triple.getSubject());
            Term p = skolemizer.getTerm(triple.getPredicate());
            Term o = skolemizer.getTerm(triple.getObject());
            assert s != null && p != null && o != null;
            list.add(new Triple(s, p, o));
        }
        return list;
    }

    private void extractFile(@Nonnull @WillClose BufferedReader reader,
                         @Nonnull String path) throws IOException {
        extractFile(reader, path, Integer.MAX_VALUE);
    }

    private void extractFile(@Nonnull @WillClose BufferedReader reader,
                             @Nonnull String path, int maxLines) throws IOException {
        File queryFile = new File(outDir, path);

        try (FileOutputStream outStream = new FileOutputStream(queryFile);
             OutputStreamWriter writer = new OutputStreamWriter(outStream, UTF_8)) {
            int linesWritten = 0;
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                writer.write(line);
                writer.write('\n');
                ++linesWritten;
                if (linesWritten >= maxLines) {
                    logger.info("Stopped extracting {} after {} lines", path, linesWritten);
                    break;
                }
            }
        } finally {
            reader.close();
        }
    }

    private final class BenchmarkQuery {
        private final @Nonnull String queryName;
        private final Supplier<InputStream> queryStreamSupplier, resultsStreamSupplier;
        private @Nullable CQuery cQuery;

        public BenchmarkQuery(@Nonnull String queryName,
                              @Nonnull Supplier<InputStream> queryStreamSupplier,
                              @Nonnull Supplier<InputStream> resultsStreamSupplier) {
            checkArgument(!queryName.isEmpty());
            this.queryName = queryName;
            this.queryStreamSupplier = queryStreamSupplier;
            this.resultsStreamSupplier = resultsStreamSupplier;
        }

        private @Nonnull InputStream getQueryStream() {
            InputStream inputStream = this.queryStreamSupplier.get();
            checkNotNull(inputStream);
            return inputStream;
        }

        private @Nonnull InputStream getResultsStream() {
            InputStream inputStream = this.resultsStreamSupplier.get();
            checkNotNull(inputStream);
            return inputStream;
        }

        private @Nonnull BufferedReader getQueryReader() {
            return new BufferedReader(new InputStreamReader(getQueryStream(), UTF_8));
        }
        private @Nonnull BufferedReader getResultsReader() {
            return new BufferedReader(new InputStreamReader(getResultsStream(), UTF_8));
        }

        private @CanIgnoreReturnValue @Nonnull
        CQuery parse() throws SPARQLParseException, IOException {
            if (cQuery == null) {
                try (BufferedReader reader = getQueryReader()) {
                    cQuery = SPARQLQueryParser.tolerant().parse(reader);
                }
            }
            return cQuery;
        }

        public @Nonnull CollectionResults parseResults() throws IOException {
            return LargeRDFBenchTestResourcesGenerator
                    .parseResults(queryName, getResultsReader(), maxResultsPerQuery);
        }
    }

    private @Nullable InputStream openFileNoThrow(@Nonnull File file) {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            logger.error("File not found: {}. openFileNoThrow() will return null.",
                         file.getAbsolutePath());
            return null;
        }
    }

    private @Nullable InputStream openEntryNoThrow(@Nonnull ZipFile zipFile, @Nonnull ZipEntry entry) {
        try {
            return zipFile.getInputStream(entry);
        } catch (IOException e) {
            logger.error("Failed to open zip entry {} in {}: {}. " +
                         "openEntryNoThrow() will return null.",
                         entry.getName(), zipFile, e.getMessage());
            return null;
        }
    }

    private Iterator<BenchmarkQuery> listPairs() throws IOException {
        if (resultsDir != null) {
            File resultsSubdir = new File(resultsDir, "Results");
            return stream(new File(resultsDir, "Queries").listFiles())
                    .map(query -> {
                        File results = new File(resultsSubdir, query.getName());
                        return new BenchmarkQuery(query.getName(),
                                () -> openFileNoThrow(query),
                                () -> openFileNoThrow(results));
                    }).iterator();
        } else if (resultsZip != null) {
            ZipFile zipFile = new ZipFile(resultsZip);
            return zipFile.stream()
                    .filter(e -> e.getName().matches("^[^/]+/Queries/.+"))
                    .map(e -> {
                        String queryName = e.getName().replaceAll("[^/]+/Queries/", "");
                        String resultsPath = e.getName().replaceAll("/Queries/", "/Results/");
                        ZipEntry resultsEntry = zipFile.getEntry(resultsPath);
                        if (resultsEntry == null) return null;
                        return new BenchmarkQuery(queryName,
                                () -> openEntryNoThrow(zipFile, e),
                                () -> openEntryNoThrow(zipFile, resultsEntry));
                    })
                    .filter(Objects::nonNull).iterator();
        }
        throw new IllegalStateException("Either resultsDir or resultsZip must be non-null");
    }

    private File downloadZip() throws IOException {
        File temp = File.createTempFile("riefederator", ".zip");
        temp.deleteOnExit();
        deleteResultsZip = true;
        logger.info("Downloading {} into {}...", ZIP_URL, temp.getAbsolutePath());
        WebTarget target = ClientBuilder.newClient().target(ZIP_URL);
        target.property(ClientProperties.FOLLOW_REDIRECTS, true);

        try (Response response = target.request("*/*").get();
             FileOutputStream out = new FileOutputStream(temp)) {
            InputStream in = response.readEntity(InputStream.class);
            IOUtils.copy(in, out);
            logger.info("Completed download of {} into {}...", ZIP_URL, temp.getAbsolutePath());
        } catch (Exception e) {
            if (!temp.delete())
                logger.warn("Failed to delete {} when handling a exception", temp);
            throw e;
        }
        return temp;
    }

    private void setupOutDir() throws IOException {
        if (outDir.exists() && !outDir.isDirectory())
            throw new IOException(outDir.getAbsolutePath()+" exists but is not an directory");
        if (!noPurgeOld && outDir.exists()) {
            logger.info("Purging OUT_DIR {}", outDir.getAbsolutePath());
            FileUtils.deleteDirectory(outDir);
        }
        if (!outDir.exists() && !outDir.mkdirs())
            throw new IOException("Could not mkdir "+outDir.getAbsolutePath());
        for (String name : asList("queries", "results", "data")) {
            File child = new File(outDir, name);
            if (child.exists() && !child.isDirectory() && !child.delete())
                throw new IOException("Failed to delete non-directory "+name+" in OUT_DIR");
            if (!child.exists() && !child.mkdirs())
                throw new IOException("Failed to create directory "+name+" in OUT_DIR");
        }
    }

    private static void showHelp(CmdLineParser parser, PrintStream out) {
        parser.printUsage(out);
    }
}
