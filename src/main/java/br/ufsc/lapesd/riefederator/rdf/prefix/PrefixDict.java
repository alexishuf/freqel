package br.ufsc.lapesd.riefederator.rdf.prefix;

import br.ufsc.lapesd.riefederator.rdf.term.URI;
import com.google.errorprone.annotations.Immutable;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.function.BiConsumer;

public interface PrefixDict {
    @Immutable
    class Shortened {
        private final boolean shortened;
        private final @Nonnull String longURI, shortPrefix;
        private final int prefixEndPos;

        public Shortened(boolean shortened, @Nonnull String longURI,
                         @Nonnull String shortPrefix, int prefixEndPos) {
            this.shortened = shortened;
            this.longURI = longURI;
            this.shortPrefix = shortPrefix;
            this.prefixEndPos = prefixEndPos;
        }

        public boolean isShortened() {
            return shortened;
        }
        public @Nonnull String getLongURI() {
            return longURI;
        }
        public @Nonnull String getPrefix() {
            return shortPrefix;
        }
        public @Nonnull String getNamespace() {
            return longURI.substring(0, getPrefixEndPos());
        }
        public @Nonnull String getLocalName() {
            return longURI.substring(getPrefixEndPos());
        }
        public int getPrefixEndPos() {
            return prefixEndPos;
        }

        @Contract(value = "!null -> !null", pure = true)
        public String toString(String fallback) {
            return shortened ? shortPrefix + ":" + getLocalName() : fallback;
        }

        @Override
        public @Nonnull String toString() {
            return toString(longURI);
        }
    }

    /**
     * And iterable of prefix names (without ":") to the URI prefixes
     *
     * @return non-null iterable (may be empty). Iteration order is unspecified
     */
    @Nonnull Iterable<Map.Entry<String, String>> entries();

    default void forEach(BiConsumer<String, String> consumer) {
        for (Map.Entry<String, String> entry : entries()) {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Shortens an URI string
     * @param uri Full URI to be shortened
     * @return Object representing shortening result
     */
    @Nonnull Shortened shorten(@Nonnull String uri);

    /**
     * Expand a prefix (pre in pre:local). Returns fallback is expansion fails
     * @param shortPrefix prefix name to be expanded
     * @param fallback fallback to return if prefix is not known
     * @return expanded URI or fallback
     */
    @Contract(value = "_, !null -> !null", pure = true)
    String expandPrefix(@Nonnull String shortPrefix, String fallback);

    default @Nonnull Shortened shorten(@Nonnull URI uri) {return shorten(uri.getURI());}

    /**
     * Tries to shorten a URL to "prefix:" and returns "prefix" or the fallback
     */
    @Contract("_, !null -> !null")
    default String shortenPrefix(@Nonnull String longPrefix, String fallback) {
        Shortened result = shorten(longPrefix);
        return result.isShortened() ? result.getPrefix() : fallback;
    }

    /**
     * Equivalent to <code>shortenPrefix(longPrefix, longPrefix)</code>
     */
    default @Nonnull String shortenPrefix(@Nonnull String longPrefix) {
        return shortenPrefix(longPrefix, longPrefix);
    }

    /**
     * Parses a shortened string ("pref:local") and return the full URI string or the fallback
     */
    @Contract("_, !null -> !null")
    String expand(@Nonnull String shortened, String fallback);
}
