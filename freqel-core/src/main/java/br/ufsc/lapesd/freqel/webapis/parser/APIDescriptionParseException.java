package br.ufsc.lapesd.freqel.webapis.parser;

import br.ufsc.lapesd.freqel.util.DictTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;

public class APIDescriptionParseException extends RuntimeException {
    private @Nullable
    DictTree map;
    private @Nullable File file;
    private @Nullable String url;
    private @Nullable String resourcePath;

    public APIDescriptionParseException(@Nonnull String message) {
        super(message);
    }

    public APIDescriptionParseException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    public @Nonnull APIDescriptionParseException setMap(@Nullable DictTree map) {
        this.map = map;
        return this;
    }

    public @Nonnull APIDescriptionParseException setFile(@Nullable File file) {
        this.file = file;
        return this;
    }

    public @Nonnull APIDescriptionParseException setUrl(@Nullable String url) {
        this.url = url;
        return this;
    }

    public @Nonnull APIDescriptionParseException setResourcePath(@Nullable String resourcePath) {
        this.resourcePath = resourcePath;
        return this;
    }

    public @Nullable
    DictTree getMap() {
        return map;
    }

    public @Nullable File getFile() {
        return file;
    }

    public @Nullable String getUrl() {
        return url;
    }

    public @Nullable String getResourcePath() {
        return resourcePath;
    }
}
