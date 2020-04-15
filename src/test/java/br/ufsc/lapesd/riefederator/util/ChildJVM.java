package br.ufsc.lapesd.riefederator.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ChildJVM implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ChildJVM.class);

    private long closeTimeoutMsecs = 500;
    private long destroyTimeoutMsecs = 4000, destroyForciblyTimeoutMsecs = 4000;

    private @Nonnull Process process;
    private @Nonnull List<String> fullCommandLine;
    private @Nullable BufferedReader stdOutReader;
    private @Nullable BufferedReader stdErrReader;

    public ChildJVM(@Nonnull Process process, @Nonnull List<String> fullCommandLine) {
        this.process = process;
        this.fullCommandLine = fullCommandLine;
    }

    private static @Nonnull ProcessBuilder createBuilder(Class<?> aClass, List<String> arguments,
                                                        @Nullable List<String> fullCommand) {
        Preconditions.checkArgument(fullCommand == null || fullCommand.isEmpty());
        fullCommand = fullCommand == null ? new ArrayList<>() : fullCommand;
        String separator = System.getProperty("file.separator");
        String javaHome = System.getProperty("java.home");
        String java = javaHome + separator + "bin" + separator + "java";
        if (SystemUtils.IS_OS_WINDOWS)
            java += ".exe";
        fullCommand.add(java);
        fullCommand.add("-cp");
        fullCommand.add(System.getProperty("java.class.path"));
        fullCommand.add(aClass.getName());
        fullCommand.addAll(arguments);
        return new ProcessBuilder(fullCommand);
    }

    public static @Nonnull ProcessBuilder createBuilder(Class<?> aClass, List<String> arguments) {
        return createBuilder(aClass, arguments, null);
    }

    public static @Nonnull ProcessBuilder createBuilder(Class<?> aClass, String... arguments) {
        return createBuilder(aClass, Arrays.asList(arguments));
    }

    public static @Nonnull ChildJVM start(Class<?> aClass, List<String> args) throws IOException {
        List<String> fullCommand = new ArrayList<>();
        Process process = createBuilder(aClass, args, fullCommand)
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .redirectError(ProcessBuilder.Redirect.PIPE)
                .redirectInput(ProcessBuilder.Redirect.PIPE)
                .start();
        return new ChildJVM(process, fullCommand);
    }

    public static @Nonnull ChildJVM start(Class<?> aClass, String... args) throws IOException {
        return start(aClass, Arrays.asList(args));
    }

    public long getCloseTimeout(@Nonnull TimeUnit unit) {
        return unit.convert(closeTimeoutMsecs, MILLISECONDS);
    }

    public void setCloseTimeout(long value, @Nonnull TimeUnit unit) {
        this.closeTimeoutMsecs = MILLISECONDS.convert(value, unit);
    }

    public long getDestroyTimeout(@Nonnull TimeUnit unit) {
        return unit.convert(destroyTimeoutMsecs, MILLISECONDS);
    }

    public void setDestroyTimeout(long value, @Nonnull TimeUnit unit) {
        this.destroyTimeoutMsecs = MILLISECONDS.convert(value, unit);
    }

    public long getDestroyForciblyTimeout(@Nonnull TimeUnit unit) {
        return unit.convert(destroyForciblyTimeoutMsecs, MILLISECONDS);
    }

    public void setDestroyForciblyTimeout(long value, @Nonnull TimeUnit unit) {
        this.destroyForciblyTimeoutMsecs = MILLISECONDS.convert(value, unit);
    }

    public @Nonnull BufferedReader getStdOutReader() {
        if (stdOutReader == null) {
            InputStream in = process.getInputStream();
            stdOutReader = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()));
        }
        return stdOutReader;
    }

    public @Nonnull BufferedReader getStdErrReader() {
        if (stdErrReader == null) {
            InputStream in = process.getErrorStream();
            stdErrReader = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()));
        }
        return stdErrReader;
    }

    @Override
    public void close() throws Exception {
        try {
            if (process.waitFor(closeTimeoutMsecs, MILLISECONDS))
                return;
            process.destroy();
            if (process.waitFor(destroyTimeoutMsecs, MILLISECONDS))
                return;
            logger.warn("Sending destroyForcibly to child process {} (command: {}).",
                    process, abbrevCommand());
            process.destroyForcibly();
            if (!process.waitFor(destroyForciblyTimeoutMsecs, MILLISECONDS)) {
                logger.error("Timed out after destroyForcibly for process {} (command: {})",
                        process, abbrevCommand());
            }
        } finally {
            if (stdOutReader != null)
                stdOutReader.close();
            if (stdErrReader != null)
                stdErrReader.close();
        }
    }

    private @Nonnull String abbrevCommand() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < fullCommandLine.size(); i++) {
            String arg = fullCommandLine.get(i);
            if (i == 2 && arg.length() > 100) arg = arg.substring(0, 100) + "...";
            if (arg.contains(" ")) arg = "\""+arg+"\"";
            b.append(arg).append(' ');
        }
        b.setLength(b.length()-1);
        return b.toString();
    }
}
