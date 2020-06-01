package br.ufsc.lapesd.riefederator.benchmark;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class QueryResultsCleanup {
    private static final Logger logger = LoggerFactory.getLogger(QueryResultsCleanup.class);
    private static final String BASENAME_HEADER = "resultsBasename";
    private static final Pattern TCSV_RX = Pattern.compile("(?i)^(.*)\\.(csv|tsv)$");

    @Option(name = "--help", aliases = {"-h"}, help = true, usage = "Shows this help")
    private boolean help;

    @Option(name = "--csv", required = true,
            usage = "Result csv file with a resultsBasename header")
    private File csvFile;

    @Option(name = "--query-results-dir", usage = "Path to dir containing query results files. " +
            "If omitted, the default is the 'result' dir alongside the csv file.")
    private File queryResultsDir;

    public static void main(String[] args) throws Exception {
        QueryResultsCleanup app = new QueryResultsCleanup();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
            if (app.queryResultsDir == null) {
                File dir = app.csvFile.getParentFile();
                String name = app.csvFile.getName().replaceAll("(?i)\\.csv$", "");
                app.queryResultsDir = dir == null ? new File(name)
                                                  : new File(dir, name);
            }
        } catch (CmdLineException |IllegalArgumentException e ) {
            System.err.println(e.getLocalizedMessage());
            printUsage(System.err, parser);
        }
        if (app.help) printUsage(System.out, parser);
        else app.run();
    }

    private static void printUsage(@Nonnull PrintStream out, @Nonnull CmdLineParser parser) {
        out.print("Usage: java -cp $JAR_PATH "+QueryResultsCleanup.class.getName()+" ");
        parser.printSingleLineUsage(out);
        out.println("Options: ");
        parser.printUsage(out);
    }

    private void run() throws Exception {
        assert csvFile != null;
        assert queryResultsDir != null;

        if (!queryResultsDir.exists()) {
            logger.warn("Results directory {} does not exist. No work", queryResultsDir);
            return;
        }
        if (queryResultsDir.exists() && !queryResultsDir.isDirectory())
            throw new IOException("Results directory "+queryResultsDir+" is not a directory");

        removeStaleFiles(getValidBaseNames());
    }

    private @Nullable Set<String> getValidBaseNames() throws IOException {
        Set<String> valid = new HashSet<>();
        try (Reader r = new InputStreamReader(new FileInputStream(csvFile), UTF_8);
             CSVParser parser = new CSVParser(r, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            if (!parser.getHeaderNames().contains(BASENAME_HEADER)) {
                logger.warn("No header {} in {}", BASENAME_HEADER, csvFile);
                return null;
            }
            for (CSVRecord row : parser)
                valid.add(row.get(BASENAME_HEADER));
        } catch (FileNotFoundException e) {
            return null;
        }
        return valid;
    }

    private void removeStaleFiles(@Nullable Set<String> valid) throws IOException {
        if (valid == null)
            return; // no work
        String[] files = queryResultsDir.list((dir, name) -> TCSV_RX.matcher(name).matches());
        if (files == null)
            throw new IOException("Failed to list files in "+queryResultsDir);
        int removed = 0, failed = 0;
        for (String file : files) {
            Matcher matcher = TCSV_RX.matcher(file);
            if (matcher.matches() && !valid.contains(matcher.group(1))) {
                File fileObj = new File(queryResultsDir, file);
                if (!fileObj.delete()) {
                    ++failed;
                    logger.error("Failed to remove stale results file {}", fileObj);
                } else {
                    ++removed;
                }
            }
        }
        logger.info("{} stale files removed and {} removal attempts failed", removed, failed);
    }
}
