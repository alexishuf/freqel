package br.ufsc.lapesd.freqel.reason.tbox.vlog;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.hdt.HDTUtils;
import br.ufsc.lapesd.freqel.hdt.query.HDTEndpoint;
import br.ufsc.lapesd.freqel.model.Triple.Position;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxMaterializer;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.util.ExtractedResource;
import br.ufsc.lapesd.freqel.util.PropertyReader;
import com.github.lapesd.rdfit.RIt;
import com.github.lapesd.rdfit.components.hdt.HDTHelpers;
import com.github.lapesd.rdfit.errors.RDFItException;
import com.github.lapesd.rdfit.iterator.RDFIt;
import com.google.common.base.Stopwatch;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.NTriplesWriter;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.hdt.HDTUtils.streamTerm;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SystemVLogMaterializer implements TBoxMaterializer {
    private static final Logger logger = LoggerFactory.getLogger(SystemVLogMaterializer.class);
    private static final Pattern WIN_VAR_RX = Pattern.compile("%(.*)%");
    private static final String RULES_SAMEAS = "rules_sameAs";
    private static final CSVFormat VLOG_MAT_FORMAT
            = CSVFormat.DEFAULT.withHeader("s", "p", "o").withSkipHeaderRecord(false);
    private static final String subClassOf = V.RDFS.subClassOf.getURI();
    private static final String subPropertyOf = V.RDFS.subPropertyOf.getURI();

    private @Nonnull final PropertyReader<File> vlogTempDir =
            new PropertyReader<File>("vlog.tempdir", new File("")) {
        @Override
        protected File parse(@Nonnull PropertySource source, @Nonnull String name,
                             @Nonnull String value) {
            if (SystemUtils.IS_OS_UNIX && value.startsWith("$")) {
                value = System.getenv(value.substring(1));
                if (value == null) value = "";
            } else if (SystemUtils.IS_OS_WINDOWS) {
                Matcher matcher = WIN_VAR_RX.matcher(value);
                if (matcher.matches()) {
                    value = System.getenv(matcher.group(1));
                    if (value == null) value = "";
                }
            }
            File dir = new File(value);
            if (dir.exists()) {
                if (!dir.isDirectory()) {
                    logger.warn("Ignoring {}={}. Path exists but is not a dir.", name, value);
                    return null;
                }
            } else if (!dir.mkdirs()) {
                logger.error("Failed to mkdir {}. Will ignore {}", value, name);
                return null;
            }
            return dir;
        }
    };
    private @Nonnull final PropertyReader<String> vlogBinary =
            new PropertyReader<String>("vlog.path", "vlog") {
        @Override
        protected String parse(@Nonnull PropertySource source, @Nonnull String name,
                             @Nonnull String path) {
            File file = new File(path);
            if (file.isAbsolute() || !file.getParentFile().equals(new File(""))) {
                if (!file.exists()) {
                    logger.error("VLog binary does not exist at {} (source: {})", path, source);
                    return null;
                }
                if (!file.canExecute()) {
                    logger.error("VLog binary does not exist at {} (source: {})", path, source);
                    return null;
                }
            }
            return path; //exists and is executable or is just the name (search in $PATH)
        }
    };
    private @Nullable File tempDir;
    private @Nullable HDT hdt;
    private @Nullable TPEndpoint endpoint;
    private boolean warnedNotLoaded = false;

    private void warnNotLoaded() {
        if (!warnedNotLoaded) {
            warnedNotLoaded = true;
            logger.warn("reasoner is not loaded. will return no data");
        }
    }

    protected @Nonnull ProcessBuilder createVLogProcessBuilder(@Nonnull String... args) {
        ArrayList<String> list = new ArrayList<>();
        list.add(vlogBinary.get());
        Collections.addAll(list, args);
        return new ProcessBuilder(list)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT);
    }

    public void setVlogTempDirParent(@Nonnull File parent) {
        if (parent.exists() && !parent.isDirectory())
            throw new IllegalArgumentException("Given parent "+parent+" is not a dir");
        vlogTempDir.setOverride(parent);
    }
    public void setVlogBinary(@Nonnull String binary) {
        vlogBinary.setOverride(binary);
    }

    @Override public void load(@Nonnull TBoxSpec sources) {
        File root = null;
        File hdtFile;
        try {
            close();
            String prefix = "freqel-vlog";
            root = Files.createTempDirectory(vlogTempDir.get().toPath(), prefix).toFile();
            root.deleteOnExit();
            File in = writeInput(root, sources);
            File kbDir = writeKB(root, in);
            File matDir = materialize(root, kbDir);
            cleanMatDir(matDir); //only TI survives
            csv2nt(new File(matDir, "TI"), in);
            try {
                FileUtils.deleteDirectory(matDir); // remove mat and TI
            } catch (IOException e) {
                logger.error("Failed to remove unneeded directory {}", matDir);
                assert false;
            }

            hdtFile = new File(root, "tbox.hdt");
            HDTHelpers.toHDTFile(hdtFile, in);
            hdt = HDTManager.mapIndexedHDT(hdtFile.getAbsolutePath());
            if (!in.delete()) {
                logger.error("Failed to delete unneeded ntriples file {}", in);
                assert false;
            }
        } catch (IOException|RDFItException e) {
            logger.error("Problem building TBox materialization HDT file at {}", root, e);
            try {
                if (root != null && root.exists())
                    FileUtils.forceDelete(root);
            } catch (IOException e2) {
                logger.error("Failed to remove {}", root, e2);
            }
            throw new VLogException(e);
        }
    }

    private @Nonnull File writeInput(@Nonnull File root,
                                     @Nonnull TBoxSpec spec) throws IOException {
        File in = new File(root, "in.nt");
        in.deleteOnExit();
        try (FileOutputStream out = new FileOutputStream(in);
             RDFIt<Triple> it = RIt.iterateTriples(Triple.class, spec.getSources())) {
            NTriplesWriter.write(out, it);
        }
        return in;
    }

    private @Nonnull File writeKB(@Nonnull File root, @Nonnull File input) throws IOException {
        File kb = new File(root, "kb");
        String kbPath = kb.getAbsolutePath(), inPath = input.getAbsolutePath();
        int timeoutSecs = 15 + (int)Math.ceil((double)input.length()/(1024*1024) * 2);
        for (int i = 0; i < 3; i++) {
            try {
                runVLog(timeoutSecs, "load", "-i", inPath, "-o", kbPath);
                return kb;
            } catch (TimeoutException e) {
                logger.warn("VLog load attempt {}/{} timed out", i+1, 3);
                if (kb.exists())
                    FileUtils.deleteDirectory(kb);
            }

        }
        runVLog("load", "-i", inPath, "-o", kbPath);
        return kb;
    }

    private @Nonnull File materialize(@Nonnull File root, @Nonnull File kb) throws IOException {
        File matDir = new File(root, "mat");
        if (!matDir.mkdirs())
            throw new IOException("Failed to mkdir "+matDir.getAbsolutePath());
        File edb = new File(root, "edb.conf");
        try (OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(edb), UTF_8)) {
            w.write("EDB0_predname=TE\nEDB0_type=Trident\nEDB0_param0=");
            w.write(kb.getAbsolutePath());
            w.write('\n');
        }
        try (ExtractedResource rules = new ExtractedResource(getClass(), RULES_SAMEAS)) {
            runVLog("mat", "--edb", edb.getAbsolutePath(),
                    "--rules", rules.getAbsolutePath(),
                    "--storemat_path", matDir.getAbsolutePath(), "--storemat_format", "csv");
        }
        try {
            FileUtils.deleteDirectory(kb);
        } catch (IOException e) {
            logger.error("Failed to delete unneeded dir {}", kb);
            assert false;
        }
        return matDir;
    }

    private void cleanMatDir(@Nonnull File matDir) {
        File[] list = matDir.listFiles((dir, name) -> !name.equals("TI"));
        if (list != null) {
            for (File file : list) {
                if (!file.delete())
                    logger.error("Failed to delete "+file.getAbsolutePath());
            }
        }
    }

    private void csv2nt(@Nonnull File csvFile,
                        @Nonnull File appendTo) throws IOException {
        StringBuilder builder = new StringBuilder();
        try (FileOutputStream outStream = new FileOutputStream(appendTo, true);
             OutputStreamWriter out = new OutputStreamWriter(outStream, UTF_8);
             InputStreamReader reader = new InputStreamReader(new FileInputStream(csvFile), UTF_8);
             CSVParser parser = CSVParser.parse(reader, VLOG_MAT_FORMAT)) {
            for (CSVRecord record : parser) {
                String subjString = csv2term(record.get(0), builder);
                if (subjString.isEmpty() || subjString.charAt(0) == '"')
                    continue; // silently ignore non-resource subjects
                out.write(subjString);
                out.write(' ');
                out.write(csv2term(record.get(1), builder));
                out.write(' ');
                out.write(csv2term(record.get(2), builder));
                out.write(" .\n");
            }
        }
    }

    private @Nonnull String csv2term(@Nonnull String value, @Nonnull StringBuilder builder) {
        if (!value.startsWith("\""))
            return value;
        builder.setLength(0);
        builder.append('"');
        boolean quoted = true, innerQuote = false, backslash = false;
        for (int i = 1; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                builder.append(c);
                backslash = true;
            } else if (backslash) {
                builder.append(c);
                backslash = false;
            } else if (quoted && c == '"') {
                if (innerQuote) {
                    innerQuote = false;
                    builder.append(c); // second quote in "", write a single "
                } else {
                    innerQuote = true; // closing " or first " in ""
                }
            } else if (quoted && innerQuote) {
                innerQuote = false;
                builder.append('"');
                builder.append(c);
                if (c == '^' || c == '@')
                    quoted = false;
            } else {
                builder.append(c);
            }
        }
        if (quoted && innerQuote)
            builder.append('"');

        String clean = builder.toString();
        int last = clean.lastIndexOf('"');
        if (last > 1 && clean.charAt(last-1) == '\\' && clean.charAt(last-2) != '\\')
            clean = clean.substring(0, last-1) + clean.substring(last);
        return clean;
    }

    private void runVLog(@Nonnull String... args) throws IOException {
        try {
            runVLog(Integer.MAX_VALUE, args);
        } catch (TimeoutException e) {
            logger.error("Ignoring VLog timeout after Integer.MAX_VALUE seconds");
        }

    }
    private void runVLog(int timeoutSecs,
                         @Nonnull String... args) throws IOException, TimeoutException {
        Process proc = createVLogProcessBuilder(args).start();
        String cmd = "VLog " + args[0];
        Stopwatch sw = Stopwatch.createStarted();
        int code = -1;
        try {
            if (!proc.waitFor(timeoutSecs, TimeUnit.SECONDS)) {
                logger.warn(cmd+"timeod out after {}s", timeoutSecs);
                proc.destroy();
                if (!waitForUninterruptbly(proc, 2, TimeUnit.SECONDS)) {
                    logger.warn(cmd+"survived destroy() after timeout, sending destroyForcibly()");
                    proc.destroyForcibly();
                    if (!waitForUninterruptbly(proc, 2, TimeUnit.SECONDS)) {
                        logger.warn(cmd+"survived destroyForcibly() after timeout");
                    }
                }
                throw new TimeoutException();
            }
            code = proc.exitValue();
            if (code != 0)
                throw new VLogException(cmd + " bad exit status: " + code);
        } catch (InterruptedException e) {
            logger.warn("Interrupted waiting for "+cmd+". Will send destroy()");
            proc.destroy();
            if (!waitForUninterruptbly(proc, 2, TimeUnit.SECONDS)) {
                logger.warn(cmd+" did not die after destroy(). Calling destroyForcibly()");
                proc.destroyForcibly();
                if (!waitForUninterruptbly(proc, 2, TimeUnit.SECONDS)) {
                    logger.warn(cmd+" did not die after destroyForcibly(). Giving up");
                }
            }
        } finally {
            logger.info("VLog with args {} took {}ms. exit status: {}",
                        args, sw.elapsed(TimeUnit.MICROSECONDS), code);
        }
    }

    private static boolean waitForUninterruptbly(@Nonnull Process process,
                                                 long value, @Nonnull TimeUnit unit) {
        try {
            return process.waitFor(value, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); //interrupt on next wait
            return false;
        }
    }

    @Override public @Nullable TPEndpoint getEndpoint() {
        if (endpoint != null)
            return endpoint;
        if (hdt == null) {
            warnNotLoaded();
            endpoint = new EmptyEndpoint();
        } else {
            endpoint = new HDTEndpoint(hdt, "VLog materialization at "+tempDir);
        }
        return endpoint;
    }

    private @Nonnull Stream<Term> querySubjects(@Nonnull String predicate, @Nonnull Term term,
                                                boolean strict) {
        if (hdt == null) {
            warnNotLoaded();
            return Stream.empty();
        }
        if (!term.isURI())
            return Stream.empty();
        try {
            String uri = HDTUtils.toHDTTerm(term);
            Stream<Term> stream = streamTerm(hdt.search(null, predicate, uri), Position.SUBJ)
                                  .filter(t -> !t.equals(term));
            if (!strict)
                stream = Stream.concat(Stream.of(term), stream);
            return stream;
        } catch (NotFoundException e) {
            return Stream.empty(); //term or predicate not in HDT (i.e., no results)
        }
    }

    @Override public @Nonnull Stream<Term> subClasses(@Nonnull Term term) {
        return querySubjects(subClassOf, term, true);
    }

    @Override public @Nonnull Stream<Term> withSubClasses(@Nonnull Term term) {
        return querySubjects(subClassOf, term, false);
    }

    @Override public @Nonnull Stream<Term> subProperties(@Nonnull Term term) {
        return querySubjects(subPropertyOf, term, true);
    }

    @Override public @Nonnull Stream<Term> withSubProperties(@Nonnull Term term) {
        return querySubjects(subPropertyOf, term, false);
    }

    @Override public boolean isSubClass(@Nonnull Term subClass, @Nonnull Term superClass) {
        if (subClass.equals(superClass))
            return true;
        if (hdt == null) {
            warnNotLoaded();
            return false;
        }
        if (!subClass.isRes()) {
            logger.warn("Suspicious call to isSubClass({}, {}): {} is not a resource",
                         subClass, superClass, subClass);
            return false;
        }
        if (superClass.isRes()) {
            logger.warn("Suspicious call to isSubClass({}, {}): {} is nto a resource",
                        subClass, superClass, superClass);
            return false;
        }
        return hasPath(subClass, subClassOf, superClass);
    }

    @Override public boolean isSubProperty(@Nonnull Term subProperty, @Nonnull Term superProperty) {
        if (subProperty.equals(superProperty))
            return true;
        if (hdt == null) {
            warnNotLoaded();
            return false;
        }
        if (!subProperty.isURI()) {
            logger.warn("Suspicious call to isSubProperty({}, {}): {} is not an IRI",
                        subProperty, superProperty, subProperty);
            return false;
        }
        if (!superProperty.isURI()) {
            logger.warn("Suspicious call to isSubProperty({}, {}): {} is not an IRI",
                        subProperty, superProperty, superProperty);
            return false;
        }
        return hasPath(subProperty, subPropertyOf, superProperty);
    }

    private boolean hasPath(@Nonnull Term startTerm, @Nonnull String predicate,
                            @Nonnull Term endTerm) {
        assert hdt != null;
        String endString = HDTUtils.toHDTTerm(endTerm);
        Set<String> visited = new HashSet<>();
        ArrayDeque<String> stack = new ArrayDeque<>();
        stack.push(HDTUtils.toHDTTerm(startTerm));
        while (!stack.isEmpty()) {
            String node = stack.pop();
            if (node.equals(endString))
                return true;
            if (!visited.add(node))
                continue;
            try {
                for (IteratorTripleString it = hdt.search(node, predicate, null); it.hasNext();)
                    stack.push(it.next().getObject().toString());
            } catch (NotFoundException ignored) { }
        }
        return false;
    }

    @Override public void close() {
        Exception exception = null;
        if (hdt != null) {
            try {
                hdt.close();
            } catch (IOException e) {
                exception = e;
            }
            hdt = null;
        }
        if (endpoint != null) {
            try {
                endpoint.close();
            } catch (Throwable t) {
                logger.error("Failed to close {}: {}", endpoint, t.getMessage(), t);
            }
        }
        if (tempDir != null) {
            try {
                FileUtils.deleteDirectory(tempDir);
            } catch (IOException e) {
                if (exception == null) exception = e;
                else                   exception.addSuppressed(e);
            }
            tempDir = null;
        }
        if (exception != null)
            throw new VLogException("Exception on close()", exception);
    }

    @Override public @Nonnull String toString() {
        return String.format("SystemVLogMaterializer@%x", System.identityHashCode(this));
    }
}
