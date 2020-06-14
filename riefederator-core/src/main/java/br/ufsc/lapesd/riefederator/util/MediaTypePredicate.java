package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class MediaTypePredicate implements Predicate<String> {
    private final @Nonnull MediaType accept;

    public MediaTypePredicate(@Nonnull MediaType accept) {
        this.accept = accept;
    }

    public MediaTypePredicate(@Nonnull String accept) {
        this(MediaType.valueOf(accept));
    }

    @Override
    public boolean test(String s) {
        MediaType mediaType = MediaType.valueOf(s);
        if (!accept.isCompatible(mediaType))
            return false;
        // isCompatible ignores parameters
        Map<String, String> params = mediaType.getParameters();
        for (Map.Entry<String, String> e : accept.getParameters().entrySet()) {
            if (!Objects.equals(params.getOrDefault(e.getKey(), null), e.getValue()))
                return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MediaTypePredicate)) return false;
        MediaTypePredicate that = (MediaTypePredicate) o;
        return accept.equals(that.accept);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accept);
    }

    @Override
    public @Nonnull String toString() {
        return accept.toString();
    }
}
