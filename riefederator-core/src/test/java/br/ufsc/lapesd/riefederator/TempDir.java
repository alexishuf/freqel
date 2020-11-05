package br.ufsc.lapesd.riefederator;

import com.google.common.base.Splitter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.util.List;

import static org.testng.Assert.assertNotNull;

public class TempDir implements Closeable {
    private @Nonnull File tempDir;

    public TempDir() throws IOException {
        tempDir = Files.createTempDirectory("riefederator").toFile();
    }

    public @Nonnull File getDir() {
        return tempDir;
    }

    public @Nonnull File getFile(String path) {
        List<String> segments = Splitter.on('/').omitEmptyStrings().splitToList(path);
        File current = new File(tempDir.getAbsolutePath());
        for (String segment : segments)
            current = new File(current, segment);
        return current;
    }

    public @Nonnull File extractStream(@Nonnull String destination,
                                       @Nonnull InputStream stream) throws IOException {
        File file = new File(tempDir.getAbsolutePath() + "/" + destination);
        File parent = file.getParentFile();
        if (!parent.exists()) {
            if (!parent.mkdirs())
                throw new IOException("Could not mkdir "+parent.getAbsolutePath());
        }
        try (FileOutputStream out = new FileOutputStream(file)) {
            IOUtils.copy(stream, out);
        }
        return file;
    }

    public @Nonnull File extractResource(@Nonnull Class<?> cls,
                                         @Nonnull String relativePath) throws IOException {
        return extractResource(cls, relativePath, null);
    }
    public @Nonnull File extractResource(@Nonnull Class<?> cls, @Nonnull String relativePath,
                                         @Nullable String destination) throws IOException {
        if (destination == null)
            destination = relativePath.replaceAll("^.*/([^/]+)$", "$1");
        try (InputStream stream = cls.getResourceAsStream(relativePath)) {
            return extractStream(destination, stream);
        }
    }

    public  @Nonnull File extractResource(@Nonnull String resourcePath) throws IOException {
        return extractResource(resourcePath, null);
    }
    public  @Nonnull File extractResource(@Nonnull String resourcePath,
                                          @Nullable String destination) throws IOException {
        if (destination == null)
            destination = resourcePath.replaceAll("^.*/([^/]+)$", "$1");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (InputStream in = cl.getResourceAsStream(resourcePath)) {
            assertNotNull(in, "resource "+resourcePath+" not found");
            return extractStream(destination, in);
        }
    }


    @Override public void close() throws IOException {
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir);
            tempDir = null;
        }
    }
}
