package br.ufsc.lapesd.freqel.util;

import com.github.lapesd.rdfit.util.Utils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

public class ExtractedResource extends File implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ExtractedResource.class);

    private static @Nonnull String extract(@Nonnull String resourcePath) throws IOException {
        InputStream is;
        try {
            is = Utils.openResource(resourcePath);
        } catch (IllegalArgumentException e) {
            String altPath = "br/ufsc/lapesd/freqel"
                           + (resourcePath.startsWith("/") ? "" : "/") + resourcePath;
            try {
                is = Utils.openResource(altPath);
            } catch (IllegalArgumentException e2) {
                throw e;
            }
        }
        return extract(is);
    }
    private static @Nonnull String extract(@Nonnull Class<?> refClass,
                                    @Nonnull String relativeResourcePath) throws IOException {
        return extract(Utils.openResource(refClass, relativeResourcePath));
    }
    private static @Nonnull String extract(@Nonnull InputStream inputStream) throws IOException {
        try {
            File file = Files.createTempFile("freqel", "").toFile();
            file.deleteOnExit();
            try (FileOutputStream out = new FileOutputStream(file)) {
                IOUtils.copy(inputStream, out);
            }
            return file.getAbsolutePath();
        } finally {
            inputStream.close();
        }
    }

    public ExtractedResource(@Nonnull String resourcePath) throws IOException {
        super(extract(resourcePath));
    }
    public ExtractedResource(@Nonnull Class<?> refClass,
                             @Nonnull String relativeResourcePath) throws IOException {
        super(extract(refClass, relativeResourcePath));
    }

    @Override public void close() {
        if (exists() && !delete())
            logger.error("Failed to delete temporary file {}. Will continue normally", this);
    }
}
