package br.ufsc.lapesd.freqel.jena.model.prefix;

import br.ufsc.lapesd.freqel.model.prefix.AbstractPrefixDict;
import br.ufsc.lapesd.freqel.model.prefix.MutablePrefixDict;
import com.google.common.base.Objects;
import org.apache.jena.shared.PrefixMapping;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class PrefixMappingDict extends AbstractPrefixDict implements MutablePrefixDict {
    private final @Nonnull PrefixMapping mapping;

    public PrefixMappingDict(@Nonnull PrefixMapping mapping) {
        this.mapping = mapping;
    }

    public @Nonnull PrefixMapping getMapping() {
        return mapping;
    }

    @Override
    public @Nonnull Iterable<Map.Entry<String, String>> entries() {
        return mapping.getNsPrefixMap().entrySet();
    }

    @Override
    public int size() {
        return mapping.numPrefixes();
    }

    @Override
    public boolean isEmpty() {
        return mapping.hasNoMappings();
    }

    @Override
    public @Nonnull Shortened shorten(@Nonnull String uri) {
        String s = mapping.shortForm(uri);
        List<String> pieces = prefixSplitter.splitToList(s);
        if (pieces.size() != 2 || mapping.getNsPrefixURI(pieces.get(0)) == null)
            return new Shortened(uri);
        return new Shortened(uri, pieces.get(0),uri.length()-pieces.get(1).length());
    }

    @Override
    public String expandPrefix(@Nonnull String shortPrefix, String fallback) {
        String uri = mapping.getNsPrefixURI(shortPrefix);
        return uri == null ? fallback : uri;
    }

    @Nonnull
    @Override
    public String toString() {
        return String.format("PrefixMappingDict({%s})",
                String.join(", ", mapping.getNsPrefixMap().keySet()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrefixMappingDict)) return false;
        PrefixMappingDict that = (PrefixMappingDict) o;
        return Objects.equal(getMapping(), that.getMapping());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getMapping());
    }

    @Override
    public @Nullable String put(@Nonnull String prefix, @Nonnull String uri) {
        String old = mapping.getNsPrefixURI(prefix);
        mapping.setNsPrefix(prefix, uri);
        return old;
    }

    @Override
    public @Nullable String remove(@Nonnull String prefix) {
        String old = mapping.getNsPrefixURI(prefix);
        mapping.removeNsPrefix(prefix);
        return old;
    }
}
