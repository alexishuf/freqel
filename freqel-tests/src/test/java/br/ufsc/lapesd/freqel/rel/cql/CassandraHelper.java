package br.ufsc.lapesd.freqel.rel.cql;

import br.ufsc.lapesd.freqel.util.ChildJVM;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.base.Splitter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CassandraHelper implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CassandraHelper.class);
    private static final Pattern RX_CREATE_KS = Pattern.compile(
            "(?i)CREATE\\s+KEYSPACE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(\\w+)");
    private static final Pattern RX_DROP_KS = Pattern.compile(
            "(?i)DROP\\s+KEYSPACE\\s+(?:IF\\s+EXISTS\\s+)?(\\w+)");
    private static final Holder holder = new Holder();
    private static final String CONTAINER_JAR = "cassandra-test-container/target/cassandra-test-container-1.0-SNAPSHOT.jar";

    private static class Holder {
        private boolean building = false;
        private @Nullable CassandraHelper helper = null;

        public Holder() {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                synchronized (Holder.this) {
                    if (helper != null) {
                        try {
                            helper.close();
                        } catch (Exception e) {
                            logger.error("Failed to close CassandraHelper {} during shutdown hook",
                                         helper, e);
                        }
                        helper = null;
                    }
                }
            }));
        }

        public synchronized void acquireForBuild() throws InterruptedException {
            while (building || helper != null)
                wait();
            building = true;
        }

        public synchronized void giveUp() {
            assert building;
            assert helper == null;
            building = false;
            notifyAll();
        }

        public synchronized void setHelper(@Nullable CassandraHelper helper) {
            assert (building && helper != null) || (!building && helper == null);
            this.helper = helper;
            building = false;
            notifyAll();
        }
    }

    private @Nonnull final CqlSession session;
    private @Nonnull final Process process;
    private @Nonnull final Set<String> keyspaces = new HashSet<>();

    protected CassandraHelper(@Nonnull CqlSession session, @Nonnull Process process) {
        this.session = session;
        this.process = process;
    }

    public static @Nonnull CassandraHelper createCassandra() throws InterruptedException, IOException {
        holder.acquireForBuild();
        try {
            CassandraHelper helper = initCassandra();
            holder.setHelper(helper);
            return helper;
        } catch (Throwable t) {
            holder.giveUp();
            throw t;
        }
    }

    private static void waitForReady(@Nonnull Process p, @Nonnull CompletableFuture<Void> future) {
        try (InputStreamReader isReader = new InputStreamReader(p.getInputStream(), UTF_8);
             BufferedReader reader = new BufferedReader(isReader)) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                System.out.println("[CASSANDRA-1] " + line);
                if (line.contains("EMBEDDED_CASSANDRA_SERVER_HELPER_READY"))
                    future.complete(null);
            }
            if (!future.isDone())
                future.completeExceptionally(new RuntimeException("Cassandra JVM died before ready"));
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
    }

    private static boolean
    waitForReadyFuture(@Nonnull Process p,
                       @Nonnull CompletableFuture<Void> future) throws IOException {
        boolean interrupted = false;
        while (true) {
            try {
                future.get();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException e) {
                try {
                    stopProcess(p, "Cassandra");
                } catch (Throwable t) {
                    logger.error("Ignoring failure to stop process {} while handling " +
                            "ExecutionException from readiness waiter thread", p, t);
                    e.addSuppressed(t);
                }
                if (e.getCause() instanceof IOException)
                    throw (IOException) e.getCause();
                else
                    throw new RuntimeException("Unexpected ExecutionException cause", e);
            }
        }
        return interrupted;
    }

    private static CassandraHelper initCassandra() throws IOException, InterruptedException {
        Process p = new ProcessBuilder(ChildJVM.getJavaPath(), "-jar", buildContainer())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start();
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread waiterThread = new Thread(() -> waitForReady(p, future));
        waiterThread.setDaemon(true);
        waiterThread.start();
        boolean interrupted = waitForReadyFuture(p, future);
        if (interrupted || Thread.interrupted()) {
            stopProcess(p, "Cassandra"); // abort creation
            throw new InterruptedException();
        }
        CqlSession session;
        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("127.0.0.1", 9142))
                    .withLocalDatacenter("datacenter1").build();
        } catch (Throwable t) {
            try {
                stopProcess(p, "Cassandra");
            } catch (Throwable t2) {
                logger.error("Ignoring failure to stop process {} while handling failure " +
                             "to get CqlSession", p, t2);
                t.addSuppressed(t2);
            }
            throw t;
        }
        return new CassandraHelper(session, p);
    }

    private static @Nonnull String buildContainer() throws IOException, InterruptedException {
        String wrapperPath = "../mvnw" + (SystemUtils.IS_OS_WINDOWS ? ".cmd" : "");
        File project = new File("cassandra-test-container");
        if (!project.exists() || !project.isDirectory()) {
            throw new FileNotFoundException(project + " not found. Running from " +
                                            "freqel parent project directory?");
        }
        Process process = new ProcessBuilder(wrapperPath, "package").directory(project)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .start();
        if (!process.waitFor(2, TimeUnit.MINUTES)) {
            logger.error("Maven wrapper {} is taking too long. Will kill and give up", process);
            stopProcess(process, "Maven wrapper ("+wrapperPath+")");
            throw new IOException("Maven wrapper for "+project+" timed out after 2 min");
        }
        if (process.exitValue() != 0)
            throw new IOException("Non-zero ("+process.exitValue()+") maven wrapper exit stattus");
        assert new File(CONTAINER_JAR).exists();
        return CONTAINER_JAR;
    }

    public @Nonnull CqlSession getSession() {
        return session;
    }

    public void executeCommands(@Nonnull String resourcePath,
                                @Nonnull Class<?> aClass) throws IOException {
        executeCommands(aClass.getResourceAsStream(resourcePath));
    }
    public void executeCommands(@Nonnull InputStream inputStream) throws IOException {
        executeCommands(IOUtils.toString(inputStream, UTF_8));

    }

    public void executeCommands(@Nonnull String string) {
        for (String cmd : Splitter.on(';').omitEmptyStrings().trimResults().splitToList(string)) {
            logger.info("CassandraHelper executing {}", cmd);
            session.execute(cmd);
            Matcher matcher = RX_CREATE_KS.matcher(cmd);
            if (matcher.find())
                keyspaces.add(matcher.group(1));
            matcher = RX_DROP_KS.matcher(cmd);
            if (matcher.find())
                keyspaces.remove(matcher.group(1));
        }
    }

    public void destroyAllKeyspaces() {
        destroyKeyspaces(keyspaces);
    }

    public void destroyKeyspaces(@Nonnull Collection<String> keyspaces) {
        for (String keyspace : keyspaces)
            executeCommands("DROP KEYSPACE IF EXISTS "+ keyspace);
    }

    @Override
    public void close() throws Exception {
//        destroyAllKeyspaces();
        session.close();
        OutputStream stream = process.getOutputStream();
        stream.write(UTF_8.encode("DIE\n").array());
        stream.flush();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            logger.warn("DIE command didn't finish Cassandra container in 30s. Sending SIGKILL");
            stopProcess(process, "Cassandra");
        }
        holder.setHelper(null);
    }

    private static void stopProcess(@Nonnull Process process,
                                    @Nonnull String what) throws InterruptedException {
        process.destroy();
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            logger.error(what +" did not finish in 5s after SIGKILL, sending SIGTERM...");
            if (!process.destroyForcibly().waitFor(5, TimeUnit.SECONDS))
                logger.error(what +" did not finish in 5s after SIGTERM. Will ignore");
        }
    }
}
