package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.spec.FederationSpecException;
import br.ufsc.lapesd.riefederator.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PlanningBenchmarks {

    @Param({"LRB",
            "B1",
            "B2",
            "B3",
            "B4",
            "B5",
            "B6",
            "B7",
            "B8",
            "C1",
            "C10",
            "C2",
            "C3",
            "C4",
            "C5",
            "C6",
            "C7",
            "C8",
            "C9",
            "S1",
            "S10",
            "S11",
            "S12",
            "S13",
            "S14",
            "S2",
            "S3",
            "S4",
            "S5",
            "S6",
            "S7",
            "S8",
            "S9",
            "BSBM",
            "query1",
            "query2",
            "query3",
            "query4", /* UNION */
            "query5",
            "query6", /* REGEX */
            "query7",  /* uses Product,Offer,Vendor,Review  & Person */
            "query8",
            "query10",
            "query11" /*UNION & unbound predicate */
    })
    private String querySetName;

    private Federation federation;
    private File tempDir;
    private List<String> sparqlQueries;
    private List<Op> parsedQueries;
    private List<Op> parseBenchmarkQueries;
    private List<Op> planBenchmarkPlans;

    private @Nonnull File extractZip(@Nonnull String zipPath) throws IOException {
        File dir = Files.createTempDirectory("riefederator").toAbsolutePath().toFile();
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        try (InputStream resStream = cl.getResourceAsStream(zipPath)) {
            if (resStream == null)
                throw new IOException("Could not open resource "+zipPath);
            try (ZipInputStream in = new ZipInputStream(resStream)) {
                for (ZipEntry e = in.getNextEntry(); e != null; e = in.getNextEntry()) {
                    File file = new File(dir, e.getName());
                    if (e.isDirectory()) {
                        if (!file.exists() && !file.mkdirs())
                            throw new IOException("Failed to mkdir " + e.getName());
                    } else {
                        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs())
                            throw new IOException("Failed to mkdir " + file.getParent());
                        try (FileOutputStream out = new FileOutputStream(file)) {
                            IOUtils.copy(in, out);
                        }
                    }
                }
            }
        }
        return dir;
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, FederationSpecException, SPARQLParseException {
        tempDir = extractZip(querySetName.matches("LRB|[SBC]\\d+") ? "lrb.zip" : "bsbm.zip");
        File[] files;
        if (querySetName.matches("LRB|BSBM"))
            files = new File(tempDir, "queries").listFiles();
        else if (querySetName.matches("query\\d+"))
            files = new File[]{new File(tempDir, "queries/" + querySetName+".sparql")};
        else
            files = new File[]{new File(tempDir, "queries/" + querySetName)};
        if (files == null)
            throw new RuntimeException("Failed to enumerate query files");

        federation = new FederationSpecLoader().load(new File(tempDir, "config.yaml"));
        sparqlQueries = new ArrayList<>(files.length);
        parsedQueries = new ArrayList<>(files.length);
        for (File file : files) {
            String sparql = IOUtils.toString(file.toURI(), StandardCharsets.UTF_8);
            sparqlQueries.add(sparql);
            parsedQueries.add(SPARQLParser.tolerant().parse(sparql));
        }

        parseBenchmarkQueries = new ArrayList<>(files.length);
        planBenchmarkPlans = new ArrayList<>(files.length);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        federation.close();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) { }
        FileUtils.deleteDirectory(tempDir);
        tempDir = null;
        federation = null;
    }

    @Benchmark
    public @Nonnull List<Op> parseBenchmark() throws SPARQLParseException {
        SPARQLParser parser = SPARQLParser.tolerant();
        parseBenchmarkQueries.clear();
        for (String sparql : sparqlQueries)
            parseBenchmarkQueries.add(parser.parse(sparql));
        return parseBenchmarkQueries;
    }

    @Benchmark
    public @Nonnull List<Op> planBenchmark() {
        planBenchmarkPlans.clear();
        for (Op query : parsedQueries) {
            Op plan = federation.plan(query);
            if (plan instanceof EmptyOp)
                throw new RuntimeException("No plan for query");
            planBenchmarkPlans.add(plan);
        }
        return planBenchmarkPlans;
    }
}
