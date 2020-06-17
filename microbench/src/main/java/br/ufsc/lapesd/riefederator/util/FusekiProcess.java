package br.ufsc.lapesd.riefederator.util;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.InetAddress.getLocalHost;

public class FusekiProcess implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(FusekiProcess.class);
    private static final Pattern READY_RX =
            Pattern.compile("\\[FusekiProcess\\.Child] FusekiServer listening at ([0-9]+)");

    private final @Nonnull ChildJVM childJVM;
    private final @Nonnull String sparqlEndpoint;
    private final @Nonnull File dataFile;
    private final @Nonnull Thread readerThread;
    private final @Nonnull CompletableFuture<Integer> waitResult;
    private final @Nonnull Queue<String> childOutput = new ConcurrentLinkedQueue<>();

    public FusekiProcess(@Nonnull Model data) throws IOException {
        int port = findPort();
        dataFile = toFile(data);
        childJVM = ChildJVM.builder(FusekiProcess.Child.class)
                .addArguments("--port", String.valueOf(port), dataFile.getAbsolutePath())
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .redirectErrorStream(true)
                .start();
        sparqlEndpoint = "http://localhost:"+port+"/ds/query";
        waitResult = new CompletableFuture<>();
        readerThread = new Thread(() -> {
            try {
                waitResult.complete(readReadyMessage());
                fetchLogs();
            } catch (Exception e) {
                waitResult.completeExceptionally(e);
            }
        });
        readerThread.start();
    }

    public @Nonnull ChildJVM getChildJVM() {
        return childJVM;
    }
    public @Nonnull String getSparqlEndpoint() {
        if (!waitForReady(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
            logger.warn("Interrupted or failed to start child process. " +
                        "SPARQL endpoint URI {} is likely unusable", sparqlEndpoint);
        }
        return sparqlEndpoint;
    }
    public @Nonnull File getDataFile() {
        return dataFile;
    }

    public @Nonnull Queue<String> getChildOutput() {
        return childOutput;
    }

    public boolean waitForReady(int timeout, TimeUnit unit) {
        try {
            waitResult.get(timeout, unit);
            return true;
        } catch (InterruptedException | TimeoutException e) {
            return false;
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to start process", e);
        }
    }

    private int readReadyMessage() throws Exception {
        BufferedReader reader = childJVM.getStdOutReader();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if (childOutput.size() > 1024)
                childOutput.remove();
            childOutput.add(line);
            Matcher matcher = READY_RX.matcher(line);
            if (matcher.matches())
                return Integer.parseInt(matcher.group(1));
        }
        // something went wrong, dump child's error stream
//        IOUtils.copy(childJVM.getStdErrReader(), System.err, StandardCharsets.UTF_8);
        return -1;
    }

    private void fetchLogs() throws IOException {
        BufferedReader reader = childJVM.getStdOutReader();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if (childOutput.size() > 1024)
                childOutput.remove();
            childOutput.add(line);
        }
    }

    private @Nonnull File toFile(Model data) throws IOException {
        File file = Files.createTempFile("riefederator-FusekiProcess", ".ttl").toFile();
        file.deleteOnExit();
        RDFDataMgr.write(new FileOutputStream(file), data, RDFFormat.TTL);
        if (getClass().desiredAssertionStatus()) {
            Model temp = ModelFactory.createDefaultModel();
            try (FileInputStream stream = new FileInputStream(file)) {
                RDFDataMgr.read(temp, stream, Lang.TTL);
            }
            assert temp.size() == data.size();
        }
        return file;
    }

    private int findPort() {
        int port = 0;
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) { }
        try (ServerSocket serverSocket = new ServerSocket(0, 50, getLocalHost())) {
            port = serverSocket.getLocalPort();
        } catch (IOException ignored) { }
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) { }
        return port;
    }

    @Override
    public void close() throws Exception {
        childJVM.close();
        if (!dataFile.delete())
            logger.debug("Failed to delete {}, child likely already deleted it.", dataFile);
        for (int i = 0; readerThread.isAlive() && i < 10; i++) {
            if (!readerThread.isInterrupted()) readerThread.interrupt();
            readerThread.join(100);
        }
        if (readerThread.isAlive())
            logger.warn("Waiter thread for {} is not ending by interrupts", childJVM);
    }

    public static class Child {
        @Option(name = "--port", required = true)
        private int port;

        @Argument
        private File file;

        public static void main(String[] args) throws Exception {
            Child app = new Child();
            CmdLineParser parser = new CmdLineParser(app);
            parser.parseArgument(args);
            app.run();
        }

        private void run() throws InterruptedException {
            Dataset ds = DatasetFactory.create(file.toURI().toString());
            FusekiServer server = FusekiServer.create().add("/ds", ds)
                    .loopback(true).port(port)
                    .build();
            server.start();
            try {
                Thread.sleep(300);
            } catch (InterruptedException ignored) {}
            System.out.printf("[FusekiProcess.Child] FusekiServer listening at %d\n", port);
            System.out.flush();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
                server.join();
                ds.close();
            }));
            Thread.currentThread().join();
        }
    }
}
