package br.ufsc.lapesd.riefederator;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;

public class ExtractedResource implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ExtractedResource.class);
    private final @Nonnull File file;
    private final @Nonnull String resourcePath;
    private boolean created = false;

    public ExtractedResource(@NotNull String systemPath) throws IOException {
        this.resourcePath = systemPath;
        file = extract(ClassLoader.getSystemClassLoader().getResourceAsStream(systemPath));
    }

    public ExtractedResource(@Nonnull Class<?> cls, @Nonnull String relativePath) throws IOException {
        this.resourcePath = cls.getName() + "::" + relativePath;
        file = extract(cls.getResourceAsStream(relativePath));
    }

    private File extract(@Nullable InputStream stream) throws IOException {
        if (stream == null)
            throw new FileNotFoundException("Could not find resource "+resourcePath);
        String suffix = "-" + resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        File file = File.createTempFile("riefederator", suffix);
        file.deleteOnExit();
        try (FileOutputStream out = new FileOutputStream(file)) {
            IOUtils.copy(stream, out);
        } catch (IOException e) {
            if (file.exists() && !file.delete()) {
                logger.error("Failed to delete {} created for resource {} after exception",
                             file, resourcePath, e);
            }

        }
        created = true;
        return file;
    }

    public @Nonnull File getFile() {
        return file;
    }

    public @Nonnull String getResourcePath() {
        return resourcePath;
    }

    @Override
    public void close() {
        if (created && !file.delete())
            logger.error("Failed to delete ExtractedResource " + resourcePath + " at" + file);
        created = false;
    }
}
