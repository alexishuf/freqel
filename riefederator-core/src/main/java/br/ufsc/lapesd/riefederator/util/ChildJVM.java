package br.ufsc.lapesd.riefederator.util;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ChildJVM implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ChildJVM.class);
    public static final @Nonnull Pattern XM_RX = Pattern.compile("(?i)(?:-Xm[xs])?(\\d+[kmg]?)");

    private long closeTimeoutMsecs = 500;
    private long destroyTimeoutMsecs = 4000, destroyForciblyTimeoutMsecs = 4000;

    private @Nonnull final Process process;
    private @Nonnull final List<String> fullCommandLine;
    private @Nullable BufferedReader stdOutReader;
    private @Nullable BufferedReader stdErrReader;

    public ChildJVM(@Nonnull Process process, @Nonnull List<String> fullCommandLine) {
        this.process = process;
        this.fullCommandLine = fullCommandLine;
    }

    public static @Nonnull Builder builder(@Nonnull Class<?> aClass) {
        return new Builder(aClass);
    }

    public static @Nonnull String getJavaPath() {
        String separator = System.getProperty("file.separator");
        String javaHome = System.getProperty("java.home");
        return javaHome + separator + "bin" + separator + "java" +
               (SystemUtils.IS_OS_WINDOWS ? ".exe" : "");
    }

    public static class Builder {
        private final @Nonnull Class<?> aClass;
        private @Nonnull final String java;
        private final List<String> appArguments = new ArrayList<>();
        private final List<String> jvmArguments = new ArrayList<>();
        private @Nonnull ProcessBuilder.Redirect inRedirect = ProcessBuilder.Redirect.PIPE,
                outRedirect = ProcessBuilder.Redirect.PIPE,
                errRedirect = ProcessBuilder.Redirect.PIPE;
        private boolean errRedirectStream = false;
        private @Nonnull final String classpath;
        private @Nonnull String xmx;
        private @Nullable String xms;
        private boolean allowReflectiveJavaLang = true;

        public Builder(@Nonnull Class<?> aClass) {
            this.aClass = aClass;
            java = getJavaPath();
            xmx = String.valueOf(Runtime.getRuntime().maxMemory());
            classpath = System.getProperty("java.class.path");
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder inheritOutAndErr() {
            redirectOutput(ProcessBuilder.Redirect.INHERIT);
            redirectError(ProcessBuilder.Redirect.INHERIT);
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder redirectOutput(@Nonnull ProcessBuilder.Redirect redirect) {
            outRedirect = redirect;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder redirectError(@Nonnull ProcessBuilder.Redirect redirect) {
            errRedirect = redirect;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder redirectErrorStream(boolean value) {
            errRedirectStream = value;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder redirectErrorStream() {
            return redirectErrorStream(true);
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder redirectInput(@Nonnull ProcessBuilder.Redirect redirect) {
            inRedirect = redirect;
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder setAllowReflectiveJavaLang(boolean value) {
            allowReflectiveJavaLang = true;
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder setXmx(@Nonnull String value) {
            Matcher matcher = XM_RX.matcher(value);
            if (!matcher.matches())
                throw new IllegalArgumentException(value + " is not valid for -Xmx");
            xmx = matcher.group(1);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder setXmx(long bytes) {
            return setXmx(String.valueOf(bytes));
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder setXms(@Nonnull String value) {
            Matcher matcher = XM_RX.matcher(value);
            if (!matcher.matches())
                throw new IllegalArgumentException(value + " is not valid for -Xms");
            xms = matcher.group(1);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder setXms(long bytes) {
            return setXmx(String.valueOf(bytes));
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addJavaArguments(@Nonnull Collection<String> args) {
            jvmArguments.addAll(args);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder addJavaArguments(@Nonnull String... args) {
            return addJavaArguments(asList(args));
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addArguments(@Nonnull Collection<String> args) {
            appArguments.addAll(args);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder addArguments(@Nonnull String... args) {
            return addArguments(asList(args));
        }

        @CanIgnoreReturnValue
        public @Nonnull ChildJVM start() throws IOException {
            List<String> fullCommand = new ArrayList<>();
            fullCommand.add(java);
            fullCommand.addAll(asList("-cp", classpath, "-Xmx"+xmx));
            if (xms != null)
                fullCommand.add("-Xms"+xms);
            if (allowReflectiveJavaLang)
                fullCommand.addAll(asList("--add-opens","java.base/java.lang=ALL-UNNAMED"));
            fullCommand.addAll(jvmArguments);
            fullCommand.add(aClass.getName());
            fullCommand.addAll(appArguments);
            ProcessBuilder processBuilder = new ProcessBuilder(fullCommand);
            processBuilder.redirectInput(inRedirect);
            processBuilder.redirectOutput(outRedirect);
            if (!errRedirectStream)
                processBuilder.redirectError(errRedirect);
            else
                processBuilder.redirectErrorStream(true);
            return new ChildJVM(processBuilder.start(), fullCommand);
        }
    }

    public @Nonnull Process getProcess() {
        return process;
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

    @Override
    public String toString() {
        return "ChildJVM["+process.toString()+"]["+abbrevCommand()+"]";
    }
}
