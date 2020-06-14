package br.ufsc.lapesd.riefederator.model.prefix;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface MutablePrefixDict extends PrefixDict {
    /**
     * Sets a mapping from the prefix to the given URI.
     *
     * @param prefix shortened prefix for uri (does not include ":")
     * @param uri expanded form of prefix
     * @return old uri mapped to this prefix
     */
    @Nullable String put(@Nonnull String prefix, @Nonnull String uri);

    /**
     * Removes the current mapping for the given prefix and returns it (or null).
     *
     * @param prefix prefix to remove expanded URI
     * @return the removed expansion for the prefix, or null if there was none
     */
    @Nullable String remove(@Nonnull String prefix);
}
