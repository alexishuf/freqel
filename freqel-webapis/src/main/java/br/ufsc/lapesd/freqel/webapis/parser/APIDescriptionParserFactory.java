package br.ufsc.lapesd.freqel.webapis.parser;

import javax.annotation.Nonnull;
import javax.annotation.WillNotClose;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface APIDescriptionParserFactory {
    @Nonnull APIDescriptionParser fromFile(@Nonnull File file) throws IOException;

    @WillNotClose
    @Nonnull APIDescriptionParser fromInputStream(@Nonnull InputStream inputStream) throws IOException;

    default @Nonnull APIDescriptionParser
    fromInputStreamThenClose(@Nonnull InputStream inputStream) throws IOException {
        try {
            return fromInputStream(inputStream);
        } finally {
            inputStream.close();
        }
    }

    @Nonnull APIDescriptionParser fromURL(@Nonnull String url) throws IOException;
    @Nonnull APIDescriptionParser fromResource(@Nonnull String resourcePath) throws IOException;
    @Nonnull APIDescriptionParser fromResource(@Nonnull Class<?> cls, @Nonnull String resourcePath)
                                  throws IOException;
}
