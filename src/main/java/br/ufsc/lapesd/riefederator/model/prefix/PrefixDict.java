package br.ufsc.lapesd.riefederator.model.prefix;

import br.ufsc.lapesd.riefederator.model.term.URI;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import org.checkerframework.checker.index.qual.Positive;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.BiConsumer;

public interface PrefixDict {
    @Immutable
    class Shortened {
        private final boolean shortened;
        private final @Nonnull String longURI;
        private final @Nullable String shortPrefix;
        private final int prefixEndPos;

        private Shortened(boolean shortened, @Nonnull String longURI,
                         @Nullable String shortPrefix, int prefixEndPos) {
            Preconditions.checkArgument(prefixEndPos >= 0, "Negative prefixEndPos!");
            assert shortened || prefixEndPos == 0
                    : "Non-zero prefixEndPos="+prefixEndPos+" but has not shortened";
            this.shortened = shortened;
            this.longURI = longURI;
            this.shortPrefix = shortPrefix;
            this.prefixEndPos = prefixEndPos;
        }
        public Shortened(@Nonnull String longURI, @Nonnull String shortPrefix, int prefixEndPos) {
            this(true, longURI, shortPrefix, prefixEndPos);
        }
        public Shortened(@Nonnull String longURI) {
            this(false, longURI, null, 0);
        }

        public boolean isShortened() {
            return shortened;
        }
        public @Nonnull String getLongURI() {
            return longURI;
        }

        /**
         * Gets the shortened prefix, without the ":". This is null iff
         * <code>isShortened()==false</code>
         */
        public @Nullable String getPrefix() {
            return shortPrefix;
        }
        /**
         * Gets the namespace portion of the URI. Returns the empty string iff
         * <code>isShortened()==false</code>
         */
        public @Nonnull String getNamespace() {
            return longURI.substring(0, getNamespaceEndPos());
        }
        /**
         * Gets the local name portion of the URI. Is empty only if the URI is empty.
         */
        public @Nonnull String getLocalName() {
            return longURI.substring(getNamespaceEndPos());
        }
        /**
         * Gets the position where the namespace portion of the URI ends and the local name begins.
         */
        public @Positive int getNamespaceEndPos() {
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

    boolean isEmpty();

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
