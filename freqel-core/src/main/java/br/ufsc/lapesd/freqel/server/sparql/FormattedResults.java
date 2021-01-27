package br.ufsc.lapesd.freqel.server.sparql;

import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

@Immutable
public class FormattedResults {
    private static final Logger logger = LoggerFactory.getLogger(FormattedResults.class);
    private final @SuppressWarnings("Immutable") @Nonnull MediaType mediaType;
    private final @SuppressWarnings("Immutable") @Nonnull byte[] bytes;

    public FormattedResults(@Nonnull MediaType mediaType, @Nonnull byte[] bytes) {
        this.mediaType = mediaType;
        this.bytes = bytes;
    }

    public @Nonnull MediaType getMediaType() {
        return mediaType;
    }

    public @Nonnull byte[] getBytes() {
        return bytes;
    }

    public @Nonnull Response.ResponseBuilder toResponse() {
        return Response.ok(getBytes(), getMediaType());
    }

    @Override
    public @Nonnull String toString() {
        String name = mediaType.getParameters().getOrDefault("charset", "UTF-8").toUpperCase();
        Charset charset;
        try {
            charset = Charset.forName(name);
        } catch (IllegalCharsetNameException e) {
            logger.warn("Bad charset name {} in MediaType {}. Using UTF-8", name, mediaType);
            charset = StandardCharsets.UTF_8;
        }
        return new String(bytes, charset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FormattedResults)) return false;
        FormattedResults that = (FormattedResults) o;
        return getMediaType().equals(that.getMediaType()) &&
                Arrays.equals(getBytes(), that.getBytes());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMediaType(), Arrays.hashCode(getBytes()));
    }
}
