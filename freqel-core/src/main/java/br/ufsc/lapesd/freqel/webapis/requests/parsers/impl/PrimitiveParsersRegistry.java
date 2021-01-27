package br.ufsc.lapesd.freqel.webapis.requests.parsers.impl;

import br.ufsc.lapesd.freqel.webapis.requests.parsers.PrimitiveParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static java.lang.System.identityHashCode;
import static java.util.Collections.binarySearch;

public class PrimitiveParsersRegistry {
    @VisibleForTesting
    static class Entry implements Comparable<Entry> {
        @Nonnull final List<String> path;
        @Nullable final PrimitiveParser parser;
        @LazyInit int hash = 0;

        public Entry(@Nonnull List<String> path, @Nonnull PrimitiveParser parser) {
            Preconditions.checkArgument(path.stream().allMatch(Objects::nonNull));
            this.path = path;
            this.parser = parser;
        }

        public Entry(@Nonnull List<String> path) {
            Preconditions.checkArgument(path.stream().allMatch(Objects::nonNull));
            this.path = path;
            this.parser = null;
        }

        @Override
        public int compareTo(@Nonnull PrimitiveParsersRegistry.Entry o) {
            assert path.stream().allMatch(Objects::nonNull);
            assert o.path.stream().allMatch(Objects::nonNull);
            Iterator<String> it = o.path.iterator();
            for (String segment : path) {
                if (!it.hasNext()) return 1;
                int diff = segment.compareTo(it.next());
                if (diff != 0) return diff;
            }
            if (it.hasNext()) return -1;

            if      (parser == null   && o.parser != null  ) return -1;
            else if (parser != null   && o.parser == null  ) return 1;
            else if (parser == null /*&& o.parser == null*/) return 0;
            else if (parser.equals(o.parser)) return 0;
            int diff = tryParserCompare(parser, o.parser);
            if (diff != 0) return diff;
            return Integer.compare(identityHashCode(parser), identityHashCode(o.parser));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private int tryParserCompare(@Nonnull PrimitiveParser l, @Nonnull PrimitiveParser r) {
            if (!(l instanceof Comparable) || !(r instanceof Comparable)) return 0;
            if (l.getClass().isAssignableFrom(r.getClass())) {
                return ((Comparable) l).compareTo(r);
            } else if (r.getClass().isAssignableFrom(l.getClass())) {
                return -1 * ((Comparable) r).compareTo(l);
            }
            return 0;
        }

        @Override
        public @Nonnull String toString() {
            String pathString = String.join("/", path);
            return pathString + (parser == null ? "" : "@" + parser.toString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Entry)) return false;
            Entry entry = (Entry) o;
            return path.equals(entry.path) &&
                    Objects.equals(parser, entry.parser);
        }

        @Override
        public int hashCode() {
            if (hash == 0)
                hash = Objects.hash(path, parser);
            return hash;
        }
    }

    private final @Nonnull List<Entry> entries = new ArrayList<>();

    @VisibleForTesting
    @Nonnull List<Entry> getEntries() {
        return entries;
    }

    public void add(@Nonnull List<String> path, @Nonnull PrimitiveParser parser) {
        Entry entry = new Entry(ImmutableList.copyOf(path), parser);
        int idx = binarySearch(entries, entry);
        if (idx >= 0) {
            entries.set(idx, entry);
        } else {
            idx = (-1 * idx) - 1;
            entries.add(idx, entry);
        }
        assert entries.get(idx).equals(entry);
    }

    public @Nullable PrimitiveParser get(@Nonnull List<String> path) {
        int idx = binarySearch(entries, new Entry(path));
        if (idx < 0) idx = (-1*idx)-1;
        if (idx >= entries.size()) return null;
        Entry entry = entries.get(idx);
        return entry.path.equals(path) ? entry.parser : null;
    }

    public @Nonnull PrimitiveParser get(@Nonnull List<String> path,
                                        @Nonnull PrimitiveParser fallback) {
        PrimitiveParser parser = get(path);
        return parser == null ? fallback : parser;
    }

    public @Nullable RDFNode parse(@Nonnull List<String> path, @Nullable String value) {
        return get(path, PlainPrimitiveParser.INSTANCE).parse(value);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("PrimitiveParsersRegistry{\n");
        for (Entry entry : entries) b.append("  ").append(entry.toString()).append('\n');
        return b.append("}").toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrimitiveParsersRegistry)) return false;
        PrimitiveParsersRegistry that = (PrimitiveParsersRegistry) o;
        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }
}
