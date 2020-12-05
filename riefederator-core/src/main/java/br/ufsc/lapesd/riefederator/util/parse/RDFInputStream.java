package br.ufsc.lapesd.riefederator.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;

import static java.lang.String.format;

public class RDFInputStream implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RDFInputStream.class);
    private @Nonnull InputStream inputStream;
    private @Nullable RDFSyntax syntax;
    private @Nonnull String baseUri;
    private @Nullable File file;

    public RDFInputStream(@Nonnull File file) throws IOException {
        this.inputStream = new FileInputStream(file);
        this.file = file;
        this.baseUri = file.getAbsoluteFile().toURI().toString()
                           .replaceFirst("^file:", "file://");
    }

    public RDFInputStream(@Nonnull InputStream inputStream) {
        this.inputStream = inputStream;
        this.baseUri = "urn:inputstream:" + System.identityHashCode(inputStream);
    }

    public @Nonnull RDFInputStream setSyntax(@Nonnull RDFSyntax syntax) {
        this.syntax = syntax;
        return this;
    }

    public void guessSyntax() {
        if (!inputStream.markSupported())
            inputStream = new BufferedInputStream(inputStream);
        RDFSyntax guess = RDFSyntax.guess(inputStream);
        if (guess != null) syntax = guess;
        else               syntax = syntax != null ? syntax : RDFSyntax.UNKNOWN;
    }

    public @Nonnull RDFInputStream setBaseUri(@Nonnull String baseUri) {
        this.baseUri = baseUri;
        return this;
    }

    public @Nonnull InputStream getInputStream() {
        return inputStream;
    }

    public @Nullable RDFSyntax getSyntax() {
        return syntax;
    }

    public @Nonnull RDFSyntax getSyntaxOrGuess() {
        if (syntax == null)
            guessSyntax();
        assert syntax != null;
        return syntax;
    }

    public @Nonnull String getBaseUri() {
        return baseUri;
    }

    @Override public @Nonnull String toString() {
        String common = "RDFInputStream{syntax=%s,base=%s";
        if (file != null)
            return format(common+",file=%s}", syntax, baseUri, file);
        else
            return format(common+",inputStream=%s}", syntax, baseUri, inputStream);
    }

    @Override public void close() {
        try {
            inputStream.close();
        } catch (IOException e) {
            logger.error("IOException on {}.close(). Ignoring.", this, e);
        }
    }
}
