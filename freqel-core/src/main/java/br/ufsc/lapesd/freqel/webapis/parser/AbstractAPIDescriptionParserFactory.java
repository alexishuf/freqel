package br.ufsc.lapesd.freqel.webapis.parser;

import br.ufsc.lapesd.freqel.util.ResourceOpener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;

public abstract class AbstractAPIDescriptionParserFactory implements APIDescriptionParserFactory {
    private static final Logger logger
            = LoggerFactory.getLogger(AbstractAPIDescriptionParserFactory.class);

    @Override
    public @Nonnull APIDescriptionParser fromFile(@Nonnull File file) throws IOException {
        try (InputStream inputStream = new FileInputStream(file)) {
            return fromInputStream(inputStream);
        } catch (APIDescriptionParseException e) {
            throw e.setFile(file);
        }
    }

    @Override
    public @Nonnull APIDescriptionParser
    fromInputStream(@Nonnull InputStream inputStream) throws IOException {
        File temp = null;
        try {
            temp = Files.createTempFile("freqel-", "").toFile();
            temp.deleteOnExit();
            Files.copy(inputStream, temp.toPath());
            return fromFile(temp);
        } finally {
            if (temp != null) {
                if (!temp.delete())
                    logger.warn("Failed to remove temp file {}", temp);
            }
        }
    }

    @Override
    public @Nonnull APIDescriptionParser fromURL(@Nonnull String url) throws IOException {
        try (InputStream inputStream = new URL(url).openStream()) {
            return fromInputStream(inputStream);
        } catch (APIDescriptionParseException e) {
            throw e.setUrl(url);
        }

    }

    @Override
    public @Nonnull APIDescriptionParser
    fromResource(@Nonnull String resourcePath) throws IOException {
        try (InputStream stream = ResourceOpener.getStream(resourcePath)) {
            return fromInputStream(stream);
        } catch (APIDescriptionParseException e) {
            throw e.setResourcePath(resourcePath);
        }
    }

    @Override
    public @Nonnull APIDescriptionParser
    fromResource(@Nonnull Class<?> cls, @Nonnull String resourcePath) throws IOException {
        try (InputStream stream = ResourceOpener.getStream(cls, resourcePath)) {
            return fromInputStream(stream);
        } catch (APIDescriptionParseException e) {
            throw e.setResourcePath(cls.getPackage().toString().replace('.', '/')
                                    +"/"+resourcePath);
        }
    }
}
