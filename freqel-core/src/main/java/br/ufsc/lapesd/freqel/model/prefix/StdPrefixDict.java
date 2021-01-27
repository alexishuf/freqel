package br.ufsc.lapesd.freqel.model.prefix;

import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ImmFullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.stream.StreamSupport;

@SuppressWarnings("WeakerAccess")
@NotThreadSafe
public class StdPrefixDict extends AbstractPrefixDict implements MutablePrefixDict {
    public static final @Nonnull PrefixDict EMPTY;
    public static final @Nonnull PrefixDict STANDARD;
    public static final @Nonnull PrefixDict DEFAULT;

    static {
        EMPTY = new StdPrefixDict();
        Builder b = builder();
        b.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        b.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        b.put("xsd", "http://www.w3.org/2001/XMLSchema#");
        b.put("owl", "http://www.w3.org/2002/07/owl#");
        b.put("shacl", "http://www.w3.org/ns/shacl#");
        STANDARD = b.build();
        b.put("foaf", "http://xmlns.com/foaf/0.1/");
        b.put("dct", "http://purl.org/dc/terms/");
        b.put("dcmit", "http://purl.org/dc/dcmitype/");
        b.put("earl", "http://www.w3.org/ns/earl#");
        b.put("time", "http://www.w3.org/2006/time#");
        b.put("owltime", "http://www.w3.org/TR/owl-time#");
        b.put("yago", "http://yago-knowledge.org/resource/");
        b.put("madsrdf", "http://www.loc.gov/mads/rdf/v1#");
        b.put("dbp", "http://dbpedia.org/property/");
        b.put("dbo", "http://dbpedia.org/ontology/");
        b.put("dbr", "http://dbpedia.org/resource/");
        b.put("dbc", "http://dbpedia.org/resource/Category:");
        b.put("skos", "http://www.w3.org/2004/02/skos/core#");
        b.put("geo", "http://www.opengis.net/ont/geosparql#");
        b.put("wgs84", "http://www.w3.org/2003/01/geo/wgs84_pos#");
        b.put("dcat", "http://www.w3.org/ns/dcat#");
        b.put("org", "http://www.w3.org/ns/org#");
        b.put("sioc", "http://rdfs.org/sioc/ns#");
        b.put("prov", "http://www.w3.org/ns/prov#");
        b.put("void", "http://rdfs.org/ns/void#");
        b.put("bibo", "http://purl.org/ontology/bibo/");
        b.put("geonames", "http://www.geonames.org/ontology#");
        b.put("ex", "http://example.org/");
        b.put("exs", "https://example.org/");
        DEFAULT = b.build();
        assert StreamSupport.stream(STANDARD.entries().spliterator(), false)
                .allMatch(e -> Objects.equals(DEFAULT.expandPrefix(e.getKey(), null),
                                              e.getValue())) : "DEFAULT missing prefixes";
    }

    private final @Nonnull List<String> uriPrefixes;
    private @Nonnull IndexSet<String> prefixNames;

    private final @Nonnull Iterable<Map.Entry<String, String>> iterable =
            new Iterable<Map.Entry<String, String>>() {
                @Override
                public @Nonnull Iterator<Map.Entry<String, String>> iterator() {
                    return new It();
                }
            };

    private final  class It implements Iterator<Map.Entry<String, String>> {
        private int nextIdx = 0;

        @Override public boolean hasNext() {
            return nextIdx < prefixNames.size();
        }

        @Override public Map.Entry<String, String> next() {
            int idx = this.nextIdx++;
            return new UnmodifiableMapEntry<>(prefixNames.get(idx), uriPrefixes.get(idx));
        }
    }

    public StdPrefixDict() {
        uriPrefixes = new ArrayList<>();
        prefixNames = ImmFullIndexSet.empty();
    }

    protected StdPrefixDict(@Nonnull IndexSet<String> prefixNames,
                               @Nonnull List<String> uriPrefixes) {
        this.uriPrefixes = uriPrefixes;
        this.prefixNames = prefixNames;
    }

    public static class Builder {
        private final @Nonnull Map<String, String> prefix2name = new HashMap<>();

        public @Nonnull StdPrefixDict.Builder put(@Nonnull String prefixName, @Nonnull String uriPrefix) {
            prefix2name.put(uriPrefix, prefixName);
            return this;
        }

        public @Nonnull StdPrefixDict build() {
            ArrayList<String> prefixes = new ArrayList<>(prefix2name.keySet());
            Collections.sort(prefixes);
            List<String> names = new ArrayList<>(prefixes.size());
            for (String prefix : prefixes)
                names.add(prefix2name.get(prefix));
            assert names.stream().noneMatch(Objects::isNull);
            IndexSet<String> namesSet = FullIndexSet.fromDistinct(names);
            return new StdPrefixDict(namesSet, prefixes);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    @Override
    public @Nullable String put(@Nonnull String prefix, @Nonnull String uri) {
        int idx = prefixNames.indexOf(prefix);
        if (idx >= 0)
            return uriPrefixes.set(idx, uri);
        idx = Math.abs(Collections.binarySearch(uriPrefixes, uri)+1);
        uriPrefixes.add(idx, uri);
        ArrayList<String> names = new ArrayList<>(prefixNames);
        names.add(idx, prefix);
        prefixNames = FullIndexSet.fromDistinct(names);
        assert prefixNames.size() == uriPrefixes.size();
        return null;
    }

    @Override
    public @Nullable String remove(@Nonnull String prefix) {
        int idx = prefixNames.indexOf(prefix);
        if (idx < 0)
            return null;
        uriPrefixes.remove(idx);
        ArrayList<String> names = new ArrayList<>(prefixNames);
        String old = names.remove(idx);
        prefixNames = FullIndexSet.fromDistinct(names);
        assert prefixNames.size() == uriPrefixes.size();
        return old;
    }

    @Override
    public @Nonnull Iterable<Map.Entry<String, String>> entries() {
        return iterable;
    }

    @Override
    public int size() {
        return prefixNames.size();
    }

    @Override
    public boolean isEmpty() {
        return prefixNames.isEmpty();
    }

    @Override
    public @Nonnull Shortened shorten(@Nonnull String uri) {
        int idx = Collections.binarySearch(uriPrefixes, uri);
        if (idx >= 0)
            return new Shortened(uri, prefixNames.get(idx), uri.length());
        // ip = (-1)*(idx+1) is the insertion point of uri in uriPrefixes.
        // Thus, the prefix at (ip-1) is the largest prefix that may be in uri
        idx = (-1)*(idx+1) - 1;
        if (idx >= 0) {
            String prefix = uriPrefixes.get(idx);
            if (uri.startsWith(prefix))
                return new Shortened(uri, prefixNames.get(idx), prefix.length());
        }
        return new Shortened(uri); // no match
    }

    @Override
    public String expandPrefix(@Nonnull String shortPrefix, String fallback) {
        assert prefixNames.size() == uriPrefixes.size();
        int idx = prefixNames.indexOf(shortPrefix);
        return idx < 0 ? fallback : uriPrefixes.get(idx);
    }
}
