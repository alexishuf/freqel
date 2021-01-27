package br.ufsc.lapesd.freqel.benchmark;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.benchmark.impl.QueryExperiment;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.util.ChildJVM;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempFile;

@SuppressWarnings("FieldMayBeFinal")
public class QueryEvaluation {
    private static final Logger logger = LoggerFactory.getLogger(QueryEvaluation.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "Show this help message")
    private boolean help;

    @Option(name = "--config", usage = "Path to yaml or json config file to be " +
            "read with FederationSpecLoader")
    private File configFile;

    @Option(name = "--csv", usage = "Path to CSV that will receive results")
    private File csvFile;

    @Option(name = "--json", usage = "File where JSON Object representing the single " +
            "QueryExperiment.Result instance will be written. The file will be truncated " +
            "if it already exists. When this option is given, --runs must be 1 " +
            "(i.e., it is not given)", forbids = {"--runs"})
    private File jsonResultFile;

    @Option(name = "--csv-truncate", forbids = {"--csv-append"},
            usage = "Truncate CSV file given to --csv before writing results")
    private boolean csvTruncate;

    @Option(name = "--csv-append", forbids = {"--csv-truncate"},
            usage = "Append to CSV file given to --csv (the default)")
    private boolean csvAppend = true;

    @Option(name = "--query-results-dir", usage = "For each query save the results in SPARQL " +
            "CSV and TSV formats. The query name will be used as the file basename",
            forbids = {"--use-path"})
    private @Nullable File queryResultsDir;

    @Option(name = "--child-jvm", usage = "Run each round in a dedicated JVM. " +
            "For each JVM all preheat runs will execute")
    private boolean childJVM;

    @Option(name = "--child-xmx", depends = {"--child-jvm"},
            usage = "Value for the -Xmx flag in the child JVM")
    private String childXmx;

    @Option(name = "--child-xms", depends = {"--child-jvm"},
            usage = "Value for the -Xms flag in the child JVM")
    private String childXms;

    @Option(name = "--runs", usage = "How many times to run and measure each query",
            forbids = {"--json"})
    private int runs = 1;

    @Option(name = "--preheat",
            usage = "How many times to run each query before collecting results")
    private int preheatRuns = 2;

    @Option(name = "--timeout-mins", usage = "Timeout for a single run of an experiment " +
            "(i.e., a query) With --preheat 2 and --runs 1, the effective timeout for a " +
            "child JVM will be 3 times the value given here. Timeouts are not enforced " +
            "without --child-jvm.")
    private int timeoutMinutes = 4;

    @Option(name = "--only-plan", usage = "Do not execute queries. Only create the plan")
    private boolean noExec = false;

    @Option(name = "-sleep-ms", usage = "Simple sleep between experiments to avoid overheating " +
            "and cumulative outside interference (e.g., piling up tasks from other processes)")
    private int sleepMs = 1000;

    @Option(name = "--use-path", usage = "Use the full file path instead of just the " +
            "filename to identify the query in the results CSV", forbids = {"--query-results-dir"})
    private boolean usePath;

    @Argument(metaVar = "SPARQL_FILE")
    File[] queryFiles;

    public static void main(String[] args) throws Exception {
        QueryEvaluation app = new QueryEvaluation();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
            if (app.childXmx != null && !ChildJVM.XM_RX.matcher(app.childXmx).matches())
                throw new IllegalArgumentException(app.childXmx + " is not a valid value for -Xmx");
            if (app.childXms != null && !ChildJVM.XM_RX.matcher(app.childXms).matches())
                throw new IllegalArgumentException(app.childXms + " is not a valid value for -Xms");
        } catch (CmdLineException|IllegalArgumentException e ) {
            System.err.println(e.getLocalizedMessage());
            printUsage(System.err, parser);
        }
        if (app.help) printUsage(System.out, parser);
        else app.run();
    }

    private static void printUsage(@Nonnull PrintStream out, @Nonnull CmdLineParser parser) {
        out.print("Usage: java -jar $JAR_PATH ");
        parser.printSingleLineUsage(out);
        out.println("Options: ");
        parser.printUsage(out);
    }

    private void run() throws Exception {
        List<ParsedQuery> queries = parseQueries();
        setupCSVOrJson();
        setupResultsDir();
        for (ParsedQuery parsedQuery : queries) {
            run(parsedQuery);
        }
    }

    private void run(@Nonnull ParsedQuery q) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        if (childJVM) {
            for (int i = 0; i < runs; i++) {
                logger.info("Starting run (in child JVM) {}/{} of {}...", i+1, runs, q.name);
                sw.reset().start();
                QueryExperiment.Result result = runInChild(i, q);
                logger.info("Completed run (in child JVM) {}/{} of {} in {}", i+1, runs, q.name, sw);
                saveResult(result);
            }
        } else {
            for (int i = 0; i < preheatRuns; i++) {
                QueryExperiment experiment = createExperiment(q.name, q.query, false);
                logger.info("Starting preheat run {}/{} of {}...", i+1, preheatRuns, q.name);
                sw.reset().start();
                experiment.execute(-1*preheatRuns + i);
                logger.info("Completed preheat run {}/{} of {} in {}", i+1, preheatRuns, q.name, sw);
                doSleep();
            }
            for (int i = 0; i < runs; i++) {
                QueryExperiment experiment = createExperiment(q.name, q.query, true);
                logger.info("Starting run {}/{} of {}...", i+1, runs, q.name);
                sw.reset().start();
                saveResult(experiment.execute(i));
                logger.info("Completed run {}/{} of {} in {}", i+1, runs, q.name, sw);
                doSleep();
            }
        }
    }

    private void doSleep() {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            logger.error("Received unexpected InterruptedException", e);
        }
    }

    private @Nonnull QueryExperiment.Result runInChild(int run,
                                                       @Nonnull ParsedQuery q) throws Exception {
        String jsonPrefix = "freqel-" + q.name;
        File jsonFile = createTempFile(jsonPrefix, ".json").toFile();
        jsonFile.deleteOnExit();
        ChildJVM.Builder builder = ChildJVM.builder(QueryEvaluation.class).inheritOutAndErr()
                .addArguments("--config", configFile.getAbsolutePath())
                .addArguments("--json", jsonFile.getAbsolutePath())
                .addArguments("--preheat", String.valueOf(preheatRuns));
        if (usePath)
            builder.addArguments("--use-path");
        else if (queryResultsDir != null)
            builder.addArguments("--query-results-dir", queryResultsDir.getAbsolutePath());
        if (noExec)
            builder.addArguments("--only-plan");
        if (childXmx != null)
            builder.setXmx(childXmx);
        if (childXms != null)
            builder.setXms(childXms);
        builder.addArguments(q.file.getAbsolutePath());

        LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
        try (ChildJVM child = builder.start()) {
            int effectiveTimeout = (preheatRuns + 1)* timeoutMinutes;
            int half = Math.max(1, effectiveTimeout / 2);
            if (!child.getProcess().waitFor(half, TimeUnit.MINUTES))
                logger.warn("Slow child process "+child);
            if (!child.getProcess().waitFor(half, TimeUnit.MINUTES))
                throw new TimeoutException("Timeout for child process "+child);
        } catch (Exception e) {
            logger.error("Failed to execute run {} of experiment {}.", run, q.name, e);
            return new QueryExperiment.Result(q.name, now, preheatRuns, runs, run, noExec);
        }
        try (InputStream stream = new FileInputStream(jsonFile);
             Reader r = new InputStreamReader(stream, UTF_8)) {
            QueryExperiment.Result result = new Gson().fromJson(r, QueryExperiment.Result.class);
            if (!jsonFile.delete())
                logger.warn("Failed to delete temp file {}", jsonFile);
            return result;
        } catch (IOException e) {
            logger.error("Failed to parse result of run {} of experiment {} from file {}.",
                         run, q.name, jsonFile.getAbsoluteFile(), e);
            return new QueryExperiment.Result(q.name, now, preheatRuns, runs, run, noExec);
        }
    }

    private void saveResult(QueryExperiment.Result result) throws IOException {
        if (jsonResultFile != null) {
            assert queryFiles.length == 1;
            assert csvFile == null;
            try (OutputStream stream = new FileOutputStream(jsonResultFile);
                 Writer writer = new OutputStreamWriter(stream, UTF_8)) {
                new GsonBuilder().setPrettyPrinting().create().toJson(result, writer);
            }
        } else if (csvFile != null) {
            // headers already written (or checked) on setupCSV
            try (CSVPrinter printer = openCSVPrinter(csvFile, true)) {
                printer.printRecord(result.toValueList());
            }
        }
    }

    private @Nonnull QueryExperiment createExperiment(String queryName, Op query,
                                                      boolean saveResults) {
        return new QueryExperiment(queryName, query, configFile, preheatRuns, runs, noExec,
                                   saveResults ? queryResultsDir : null);
    }

    private void setupCSVOrJson() throws IOException {
        if (csvFile != null) {
            assert jsonResultFile == null;
            File dir = csvFile.getParentFile();
            if (dir != null && !dir.exists() && !dir.mkdirs())
                throw new IOException("Could not mkdir " + dir.getAbsolutePath());
            if (csvFile.exists() && !csvFile.isFile())
                throw new IOException(csvFile.getAbsolutePath() + " is not a file!");
            if (csvTruncate || !csvFile.exists()) {
                try (CSVPrinter printer = openCSVPrinter(csvFile, false)) {
                    printer.printRecord(QueryExperiment.HEADERS);
                }
            } else {
                assert csvAppend;
                try (CSVParser parser = openCSVParser(csvFile)) {
                    if (!parser.getHeaderNames().equals(QueryExperiment.HEADERS))
                        throw new IllegalArgumentException("headers mismatch at " + csvFile);
                }
            }
            assert csvFile.exists() && csvFile.isFile() && csvFile.length() > 0;
        } else if (jsonResultFile != null) {
            File dir = jsonResultFile.getParentFile();
            if (dir != null && !dir.exists() && !dir.mkdirs())
                throw new IOException("Could not mkdir "+dir.getAbsolutePath());
            if (jsonResultFile.exists() && !jsonResultFile.isFile())
                throw new IOException(jsonResultFile.getAbsolutePath()+" is not a file!");
        }
    }

    private void setupResultsDir() throws IOException {
        if (queryResultsDir != null) {
            if (!queryResultsDir.exists() && !queryResultsDir.mkdirs())
                throw new IOException("Failed to mkdir " + queryResultsDir.getAbsolutePath());
            if (queryResultsDir.exists() && !queryResultsDir.isDirectory())
                throw new IllegalArgumentException(queryResultsDir + " is not a directory!");
        }
    }

    private @Nonnull CSVPrinter openCSVPrinter(@Nonnull File file,
                                               boolean append) throws IOException {
        FileOutputStream out = new FileOutputStream(file, append);
        OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
        return new CSVPrinter(writer, CSVFormat.DEFAULT);
    }
    private @Nonnull CSVParser openCSVParser(@Nonnull File file) throws IOException {
        FileInputStream stream = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(stream, UTF_8);
        return new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
    }

    private static class ParsedQuery {
        private final @Nonnull File file;
        private final @Nonnull String name;
        private final @Nonnull Op query;

        public ParsedQuery(@Nonnull File file, @Nonnull String name, @Nonnull Op query) {
            this.file = file;
            this.name = name;
            this.query = query;
        }
    }
    private @Nonnull List<ParsedQuery> parseQueries()
            throws SPARQLParseException, IOException {
        List<ParsedQuery> list = new ArrayList<>();
        for (File file : queryFiles) {
            Op query = SPARQLParser.tolerant().parse(file);
            String name = usePath ? file.getPath() : file.getName();
            list.add(new ParsedQuery(file, name, query));
        }
        return list;
    }
}
