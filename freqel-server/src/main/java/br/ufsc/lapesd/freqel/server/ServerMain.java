package br.ufsc.lapesd.freqel.server;

import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecException;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.server.endpoints.SPARQLEndpoint;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.naturalOrder;

@SuppressWarnings({"FieldMayBeFinal", "MismatchedQueryAndUpdateOfCollection"})
public class ServerMain {
    private static Logger logger = LoggerFactory.getLogger(ServerMain.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "Shows help", help = true)
    private boolean help = false;

    @Option(name = "--address", usage = "Server listen address")
    private @Nonnull String listenAddress = "0.0.0.0";

    @Option(name = "--port", aliases = {"-p"}, usage = "Server listen port")
    private int port = 4040;

    @Option(name = "--verbose", aliases = {"-v"}, usage = "Increase log verbosity to DEBUG " +
            "(less verbose than TRACE) on freqel and rdfit code only")
    private boolean verbose;

    @Option(name = "--extra-verbose", aliases = {"-V"}, usage = "Increase log verbosity to " +
            "TRACE level on all loggers. This will generate a lot of messages. Use only in " +
            "case of extrema despair")
    private boolean reallyVerbose;

    @Option(name = "--purge-index", usage = "Remove any indices (including ASK caches) " +
            "for sources before proceeding with a re-index (whether --only-index was give or not)")
    private boolean purgeIndex;

    @Option(name = "--only-purge-index", usage = "Apply the effect of --purge-index and exit " +
            "without indexing nor running the SPARQL endpoint")
    private boolean onlyPurgeIndex;

    @Option(name = "--only-index-seconds", usage = "Wait at most this number of seconds for all " +
            "indexes to be updated and saved to cache (for indices that allow a priori updates).")
    private int onlyIndexSeconds = 600;

    @Option(name = "--only-index", usage = "Only index the sources which need indexing and exit",
            forbids = {"--only-gen-config"})
    private boolean onlyIndex;

    @Option(name = "--gen-config", usage = "Do not run a server, instead create a config " +
            "file (as given by --config) with one or more sources (as given by other options).")
    private boolean generateConfig;

    @Option(name = "--only-gen-config", usage = "Same as --gen-config, but exit after it is done " +
            "and before starting a SPARQL endpoint", forbids = {"--only-index"})
    private boolean onlyGenerateConfig;

    @Option(name = "--select", usage = "Add the given SPARQL endpoint URL " +
            "as a source to be indexed a priori with a SelectDescription that fetches " +
            "predicates and classes.")
    private @Nonnull List<String> selectWithClasses = new ArrayList<>();

    @Option(name = "--select-without-classes", usage = "Add the given SPARQL endpoint URL as a source to be " +
            "indexed a priori with SelectDescription that fetches only the predicates.")
    private @Nonnull List<String> selectWithoutClasses = new ArrayList<>();

    @Option(name = "--ask", usage = "Add the given SPARQL endpoint URL as a source to be " +
            "indexed on the fly with AskDescription")
    private @Nonnull List<String> ask = new ArrayList<>();

    @Option(name = "--config", usage = "JSON or YAML with configuration for the federation",
            required = true)
    private File config;


    private static void printHelp(@Nonnull PrintStream out, @Nonnull CmdLineParser parser) {
        out.print("Usage: java -jar $JAR_PATH ");
        parser.printSingleLineUsage(out);
        out.println("Options: ");
        parser.printUsage(out);
    }

    public static void main(String[] args) {
        ServerMain app = new ServerMain();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getLocalizedMessage());
            printHelp(System.err, parser);
            System.exit(1);
        }
        if (app.help) {
            printHelp(System.out, parser);
        } else {
            try {
                app.run();
            } catch (Exception e) {
                logger.error("ServerMain failed with {}. args={}", e, args, e);
                System.exit(2);
            }
        }
    }

    @Nonnull private Federation getFederation() throws IOException, FederationSpecException {
        return new FederationSpecLoader().load(config);
    }

    public void run() throws Exception {
        applyVerbosity();
        if (generateConfig || onlyGenerateConfig) {
            generateConfig();
            if (onlyGenerateConfig)
                return;
        }
        Federation federation = getFederation();
        if (purgeIndex || onlyPurgeIndex) {
            purgeIndex(federation);
            if (onlyPurgeIndex)
                return;
            federation.close();
            federation = getFederation();
        }
        if (onlyIndex) {
            fillIndex(federation);
            federation.close();
            return; // to not spin SPARQL endpoint
        }
        SPARQLEndpoint ep = new SPARQLEndpoint(federation);
        DisposableServer server = HttpServer.create()
                .host(listenAddress)
                .port(port)
                .route(routes -> routes
                        .get("/sparql", ep::handle)
                        .post("/sparql", ep::handle)
                        .get("/sparql/query", ep::handle)
                        .post("/sparql/query", ep::handle))
                .bindNow();
        printBox("SPARQL endpoint listening at http://%1$s:%2$d/sparql\n" +
                "Accepted query methods are GET and form-encoded or plain POST\n" +
                "GUI is at http://%1$s:%2$d/ui/index.html", listenAddress, port);
        server.onDispose().block();
    }

    @SuppressWarnings("SameParameterValue")
    private void printBox(@Nonnull String format, Object... args) {
        String expanded = String.format(format, args);
        String[] lines = expanded.split("\n");
        int innerWidth = Arrays.stream(lines).map(String::length).max(naturalOrder()).orElse(40);
        printBoxLine(innerWidth);
        for (String line : lines) {
            System.out.append("| ").append(line);
            for (int i = line.length(); i < innerWidth; i++) System.out.append(' ');
            System.out.append(" |\n");
        }
        printBoxLine(innerWidth);
    }

    private void printBoxLine(int innerWidth) {
        System.out.print("+-");
        for (int i = 0; i < innerWidth; i++) System.out.append('-');
        System.out.print("-+\n");
    }

    private void applyVerbosity() {
        String rootName = LogManager.getRootLogger().getName();
        if (reallyVerbose) {
            Configurator.setLevel(rootName, Level.TRACE);
            Configurator.setLevel("br.ufsc.lapesd.freqel", Level.TRACE);
        } else if (verbose) {
            Configurator.setLevel("br.ufsc.lapesd.freqel", Level.DEBUG);
            Configurator.setLevel("br.ufsc.lapesd.freqel.server", Level.TRACE);
            Configurator.setLevel("br.ufsc.lapesd.freqel.description", Level.TRACE);
        }
    }

    private @Nonnull Map<String, Object> createSource(@Nonnull String url,
                                                      @Nonnull String description,
                                                      boolean fetchClasses) {
        Map<String, Object> map = new HashMap<>();
        map.put("loader", "sparql");
        map.put("uri", url);
        map.put("description", description);
        if (description.equals("select"))
            map.put("fetchClasses", fetchClasses);
        return map;
    }

    private void generateConfig() throws IOException, FreqelConfig.InvalidValueException {
        DictTree tree;
        if (config.exists()) {
            if (!config.isFile()) {
                logger.error("Config file {} is not a exists as non-file.", config);
                throw new IllegalArgumentException("Config file "+config+" is not a file!");
            }
            try {
                tree = DictTree.load().fromFile(this.config);
            } catch (IOException e) {
                logger.error("Could not read/parse existing config file {}", config);
                throw e;
            }
            try {
                FreqelConfig ignored = new FreqelConfig(tree);
            } catch (FreqelConfig.InvalidValueException e) {
                logger.error("Invalid values present in existing config file {}", config);
                throw e;
            }
            int nSources = tree.getListNN("sources").size();
            logger.info("Will overwrite {} sources in {}. Other top-level properties " +
                        "will be preserved. Comments will be lost", nSources, config);
        } else {
            File parent = config.getParentFile() == null ? new File(".") : config.getParentFile();
            if (!parent.exists() && !parent.mkdirs())
                throw new IOException("Could not mkdir "+config+" parent directory");
            tree = new DictTree();
        }
        List<Map<String, Object>> sources = new ArrayList<>();
        for (String url : selectWithClasses)
            sources.add(createSource(url, "select", true));
        for (String url : selectWithoutClasses)
            sources.add(createSource(url, "select", false));
        for (String url : ask)
            sources.add(createSource(url, "ask", false));
        tree.put("sources", sources);
        try (FileOutputStream outputStream = new FileOutputStream(config);
             OutputStreamWriter osWriter = new OutputStreamWriter(outputStream, UTF_8)) {
            YamlWriter w = new YamlWriter(osWriter);
            w.getConfig().setClassTag("tag:yaml.org,2002:map", LinkedHashMap.class);
            w.write(tree.asMap());
            w.close();
        }
        logger.info("Wrote new {} with {} sources: {} predicates&classes SELECT-indexed, " +
                    "{} predicates SELECT-indexed and {} ASK-indexed", config, sources.size(),
                    selectWithClasses.size(), selectWithoutClasses.size(), ask.size());
    }

    private void fillIndex(@Nonnull Federation federation) throws TimeoutException {
        for (TPEndpoint source : federation.getSources())
            source.getDescription().init(); //only if missing or !TRUST_SOURCE_CACHE
        int maxMs = onlyIndexSeconds*1000;
        Stopwatch sw = Stopwatch.createStarted();
        int complete = 0, nSources = federation.getSources().size();
        for (TPEndpoint source : federation.getSources()) {
            long rem = Math.max(0, maxMs - (int)sw.elapsed(TimeUnit.MILLISECONDS));
            if (source.getDescription().waitForInit((int)rem))
                ++complete;
        }
        if (complete < nSources) {
            String msg = String.format("%d/%d sources failed to index in %d seconds",
                                       nSources-complete, nSources, onlyIndexSeconds);
            throw new TimeoutException(msg);
        } else {
            logger.info("Indexed {} sources in {}. Exiting", nSources, sw);
        }
    }

    private void purgeIndex(Federation federation) {
        File dir = federation.getSourceCache().getDir();
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files == null) {
                logger.error("Could not list {}, cannot purge!", dir);
            } else {
                List<String> failed = new ArrayList<>();
                Path root = dir.toPath();
                for (File file : files) {
                    if (file.isDirectory()) {
                        try {
                            FileUtils.deleteDirectory(file);
                        } catch (IOException e) {
                            if (file.exists())
                                failed.add(root.relativize(file.toPath()).toString());
                        }
                    } else if (!FileUtils.deleteQuietly(file)) {
                        failed.add(root.relativize(file.toPath()).toString());
                    }
                }
                if (!failed.isEmpty()) {
                    logger.error("Failed to delete some files/dirs in {}: {}",
                                 dir, String.join(", ", failed));
                }
            }
        } else {
            logger.info("Non-existing cache dir {}, nothing to purge", dir);
        }
    }


}
