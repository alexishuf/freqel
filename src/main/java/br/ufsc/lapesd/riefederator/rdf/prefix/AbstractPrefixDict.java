package br.ufsc.lapesd.riefederator.rdf.prefix;

import com.google.common.base.Splitter;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.List;

public abstract class AbstractPrefixDict implements PrefixDict {
    protected static final Splitter prefixSplitter = Splitter.onPattern("(?<!\\\\):").limit(2);

    @Override
    @Contract("_, !null -> !null")
    public @Nonnull String expand(@Nonnull String shortened, String fallback) {
        List<String> pieces = prefixSplitter.splitToList(shortened);
        if (pieces.size() != 2) return fallback;
        String expanded = expandPrefix(pieces.get(0), null);
        return expanded == null ? fallback : expanded + pieces.get(1);
    }

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder();
        b.append("{");
        forEach((prefix, uri) ->
                b.append("  ").append(prefix).append(" --> ").append(uri).append("\n"));
        return b.append("}").toString();
    }
}
