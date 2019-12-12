package br.ufsc.lapesd.riefederator.model.prefix;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.jena.sparql.vocabulary.EARL;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.DCTypes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

@SuppressWarnings("WeakerAccess")
@NotThreadSafe
public class StdPrefixDict extends AbstractPrefixDict implements MutablePrefixDict {
    private Map<String, String> prefix2URI = new HashMap<>();
    private PatriciaTrie<String> uri2Prefix = new PatriciaTrie<>();

    public static final @Nonnull PrefixDict EMPTY;
    public static final @Nonnull PrefixDict STANDARD;
    public static final @Nonnull PrefixDict DEFAULT;

    static {
        EMPTY = new StdPrefixDict();
        StdPrefixDict std = new StdPrefixDict();
        std.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        std.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        std.put("xsd", "http://www.w3.org/2001/XMLSchema#");
        std.put("owl", "http://www.w3.org/2002/07/owl#");
        std.put("shacl", "http://www.w3.org/ns/shacl#");
        STANDARD = std;
        StdPrefixDict def = new StdPrefixDict();
        std.entries().forEach(e -> def.put(e.getKey(), e.getValue()));
        def.put("foaf", "http://xmlns.com/foaf/0.1/");
        def.put("dct", "http://purl.org/dc/terms/");
        def.put("dcmit", "http://purl.org/dc/dcmitype/");
        def.put("earl", "http://www.w3.org/ns/earl#");
        def.put("time", "http://www.w3.org/2006/time#");
        def.put("owltime", "http://www.w3.org/TR/owl-time#");
        def.put("yago", "http://yago-knowledge.org/resource/");
        def.put("madsrdf", "http://www.loc.gov/mads/rdf/v1#");
        def.put("dbp", "http://dbpedia.org/property/");
        def.put("dbo", "http://dbpedia.org/ontology/");
        def.put("dbr", "http://dbpedia.org/resource/");
        def.put("dbc", "http://dbpedia.org/resource/Category:");
        def.put("skos", "http://www.w3.org/2004/02/skos/core#");
        def.put("geo", "http://www.opengis.net/ont/geosparql#");
        def.put("wgs84", "http://www.w3.org/2003/01/geo/wgs84_pos#");
        def.put("dcat", "http://www.w3.org/ns/dcat#");
        def.put("org", "http://www.w3.org/ns/org#");
        def.put("sioc", "http://rdfs.org/sioc/ns#");
        def.put("prov", "http://www.w3.org/ns/prov#");
        def.put("void", "http://rdfs.org/ns/void#");
        def.put("bibo", "http://purl.org/ontology/bibo/");
        def.put("geonames", "http://www.geonames.org/ontology#");
        def.put("ex", "http://example.org/");
        def.put("exs", "https://example.org/");
        DEFAULT = def;
    }

    @Override
    public @Nonnull Iterable<Map.Entry<String, String>> entries() {
        return prefix2URI.entrySet();
    }

    @Override
    public boolean isEmpty() {
        return prefix2URI.isEmpty();
    }

    @Override
    public synchronized  @Nonnull Shortened shorten(@Nonnull String uri) {
        // TODO remove synchronized from here after replacing PatriciaTrie (lookups may cause a remove followed by add)
        String key = uri, prefix = uri2Prefix.get(uri);
        if (prefix == null) {
            SortedMap<String, String> map = uri2Prefix.headMap(uri);
            //if a prefix of uri is mapped, the longest will be the lastKey()
            if (!map.isEmpty() && uri.startsWith(key = map.lastKey()))
                prefix = map.get(key); // lastKey() is indeed a prefix of uri
        }
        return prefix != null ? new Shortened(uri, prefix, key.length()) : new Shortened(uri);
    }

    @Override
    public String expandPrefix(@Nonnull String shortPrefix, String fallback) {
        return prefix2URI.getOrDefault(shortPrefix, fallback);
    }

    @Override
    public synchronized @Nullable String put(@Nonnull String prefix, @Nonnull String uri) {
        String old = prefix2URI.put(prefix, uri);
        uri2Prefix.remove(old, prefix);
        uri2Prefix.put(uri, prefix);
        return old;
    }

    @Override
    public synchronized @Nullable String remove(@Nonnull String prefix) {
        String uri = prefix2URI.remove(prefix);
        if (uri != null)
            uri2Prefix.remove(uri);
        return uri;
    }
}
