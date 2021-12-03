package br.ufsc.lapesd.freqel.rel.cql;

import br.ufsc.lapesd.freqel.util.ChildJVM;
import br.ufsc.lapesd.freqel.util.ProcessUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.base.Splitter;
import org.apache.commons.io.IOUtils;
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
            logger.error("Cassandra initialization failed", t);
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
                    ProcessUtils.stopProcess(p, "Cassandra");
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
        String javaPath = ChildJVM.getJavaOlderThan(15);
        if (javaPath == null) {
            throw new AssertionError("Running the cassandra test container under " +
                    "OpenJDK/GraalVM >= 15 will hang. Tried but could not guess the location of " +
                    "an older JVM on this machine");
        }
        File jar = ProcessUtils.mavenWrapperBuild(
                "cassandra-test-container", "../",
                "cassandra-test-container-1.0-SNAPSHOT.jar", "package");
        Process p = new ProcessBuilder()
                .command(javaPath, "-jar", jar.getAbsolutePath())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start();
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread waiterThread = new Thread(() -> waitForReady(p, future));
        waiterThread.setDaemon(true);
        waiterThread.start();
        boolean interrupted = waitForReadyFuture(p, future);
        if (interrupted || Thread.interrupted()) {
            ProcessUtils.stopProcess(p, "Cassandra"); // abort creation
            throw new InterruptedException();
        }
        CqlSession session;
        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("127.0.0.1", 9142))
                    .withLocalDatacenter("datacenter1").build();
        } catch (Throwable t) {
            try {
                ProcessUtils.stopProcess(p, "Cassandra");
            } catch (Throwable t2) {
                logger.error("Ignoring failure to stop process {} while handling failure " +
                             "to get CqlSession", p, t2);
                t.addSuppressed(t2);
            }
            throw t;
        }
        return new CassandraHelper(session, p);
    }

    public @Nonnull CqlSession getSession() {
        return session;
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
        ProcessUtils.waitOrStopProcess(process, 30, TimeUnit.SECONDS, "Cassandra");
        holder.setHelper(null);
    }
}
