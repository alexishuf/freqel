package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.util.ProcessUtils;
import com.github.lapesd.rdfit.components.hdt.HDTHelpers;
import com.github.lapesd.rdfit.components.jena.JenaHelpers;
import com.github.lapesd.rdfit.source.RDFInputStream;
import org.apache.jena.riot.Lang;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.freqel.util.ChildJVM.getJavaPath;

public class HDTSSProcess implements AutoCloseable {
    private static final @Nonnull AtomicInteger nextId = new AtomicInteger(1);
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(HDTSSProcess.class);
    private final int id;
    private Thread portFetcher;
    private Process process = null;
    private String cmdLineString = null;
    private int port = 0;
    private @Nullable final File disposableFile;

    @SuppressWarnings("unused")
    public HDTSSProcess(@Nonnull String hdtFile) throws IOException {
        this(hdtFile, false);
    }

    public HDTSSProcess(@Nonnull String hdtFilePath, boolean deleteOnClose)
            throws IOException {
        File hdtFile = new File(hdtFilePath).getAbsoluteFile();
        if (!hdtFile.isFile())
            throw new FileNotFoundException("No such file: "+hdtFilePath);
        id = nextId.getAndIncrement();
        disposableFile = deleteOnClose ? hdtFile : null;
        hdtFilePath = hdtFile.getAbsolutePath();

        try {
            File jar = ProcessUtils.mavenWrapperBuild(
                    "hdtss", null, "hdtss",
                    "package", "-DskipTests=true");
            List<String> cmd = Arrays.asList(getJavaPath(), "-jar", jar.getAbsolutePath(),
                    "-port=-1", hdtFilePath);
            cmdLineString = String.join(" ", cmd);
            boolean interrupted = false, ready = false;
            for (int i = 0; i < 4 && !ready; ++i) {
                process = new ProcessBuilder(cmd)
                        .redirectOutput(ProcessBuilder.Redirect.PIPE)
                        .redirectErrorStream(true)
                        .start();
                CompletableFuture<Integer> portFuture = new CompletableFuture<>();
                portFetcher = new Thread(() -> handleHDTSSOutput(portFuture));
                portFetcher.setDaemon(true);
                portFetcher.start();
                while (port == 0) {
                    try {
                        port = portFuture.get();
                        ready = true;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    } catch (ExecutionException ee) {
                        if (ee.getCause() instanceof RetryHDTSSException) {
                            ProcessUtils.stopProcess(process, cmdLineString);
                            portFetcher.join();
                        } else {
                            throw ee;
                        }
                    }
                }
            }
            if (!ready)
                throw new RuntimeException("Could not start hdtss after 4 attempts");
            if (interrupted)
                Thread.currentThread().interrupt();
        } catch (Throwable t) {
            if (process != null)
                ProcessUtils.stopProcess(process, cmdLineString);
            if (deleteOnClose && !hdtFile.delete())
                logger.error("Failed to delete temp file {}", hdtFilePath);
            if (t instanceof Error)
                throw (Error)t;
            if (t instanceof RuntimeException)
                throw (RuntimeException)t;
            throw new RuntimeException(t);
        }
    }

    private static class RetryHDTSSException extends RuntimeException {}

    private void handleHDTSSOutput(CompletableFuture<Integer> portFuture) {
        Pattern conflictRx = Pattern.compile("(?i)Unable to start server .* Port .* already .* in use");
        Pattern okRx = Pattern.compile("(?i)server running.*http://[^:]+:(\\d+)");
        try (InputStream is = process.getInputStream();
             BufferedReader r = new BufferedReader(new InputStreamReader(is))) {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                if (!portFuture.isDone()) {
                    Matcher conflictMatcher = conflictRx.matcher(line);
                    if (conflictMatcher.find())
                        portFuture.completeExceptionally(new RetryHDTSSException());
                    Matcher okMatcher = okRx.matcher(line);
                    if (okMatcher.find())
                        portFuture.complete(Integer.parseInt(okMatcher.group(1)));
                }
                System.out.printf("[HDTSS-%d port %d] %s\n", id, port, line);
            }
        } catch (Throwable t) {
            if (!portFuture.isDone())
                portFuture.completeExceptionally(t);
        }
    }

    public static @Nonnull HDTSSProcess forRDF(@Nonnull Object rdfSource)  {
        try {
            File tempFile = Files.createTempFile("freqel", ".hdt").toFile();
            tempFile.deleteOnExit();
            File hdtFile = HDTHelpers.toHDTFile(tempFile, rdfSource);
            return new HDTSSProcess(hdtFile.getAbsolutePath(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static @Nonnull HDTSSProcess forRDF(@Nonnull InputStream in, @Nonnull Lang lang)  {
        return forRDF(new RDFInputStream(in, JenaHelpers.fromJenaLang(lang)));
    }

    public @Nonnull String getEndpoint() {
        return "http://127.0.0.1:"+port+"/sparql";
    }

    @Override public void close() {
        if (process == null)
            return;
        ProcessUtils.stopProcess(process, cmdLineString, 10, 5);
        process = null;
        try {
            portFetcher.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (portFetcher.isAlive()) {
            logger.warn("The portFetcher thread {} still alive 5s after process was killed. " +
                        "Will leak the thread", portFetcher.getName());
        }
        if (disposableFile != null && !disposableFile.delete())
            logger.error("Failed to delete temp file {} on close()", disposableFile);
    }
}
