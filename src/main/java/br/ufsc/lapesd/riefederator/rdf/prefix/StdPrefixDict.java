package br.ufsc.lapesd.riefederator.rdf.prefix;

import org.apache.commons.collections4.trie.PatriciaTrie;

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

    static {
        EMPTY = new StdPrefixDict();
        StdPrefixDict std = new StdPrefixDict();
        std.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        std.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        std.put("xsd", "http://www.w3.org/2001/XMLSchema#");
        std.put("owl", "http://www.w3.org/2002/07/owl#");
        std.put("shacl", "http://www.w3.org/ns/shacl#");
        STANDARD = std;
    }

    @Override
    public @Nonnull Iterable<Map.Entry<String, String>> entries() {
        return prefix2URI.entrySet();
    }

    @Override
    public @Nonnull Shortened shorten(@Nonnull String uri) {
        SortedMap<String, String> map = uri2Prefix.headMap(uri);
        String key = uri, prefix;
        if (map.isEmpty()) {
            prefix = uri2Prefix.get(key); //uri matches entirelly to a prefix
        } else {
            key = map.lastKey(); //longest subs-string of uri mapped to a prefix
            prefix = map.get(key);
        }
        if (prefix != null) //got a match
            return new Shortened(true, uri, prefix, key.length());
        return new Shortened(false, uri, "", 0);
    }

    @Override
    public String expandPrefix(@Nonnull String shortPrefix, String fallback) {
        return prefix2URI.getOrDefault(shortPrefix, fallback);
    }

    @Override
    public @Nullable String put(@Nonnull String prefix, @Nonnull String uri) {
        String old = prefix2URI.put(prefix, uri);
        uri2Prefix.remove(old, prefix);
        uri2Prefix.put(uri, prefix);
        return old;
    }

    @Override
    public @Nullable String remove(@Nonnull String prefix) {
        String uri = prefix2URI.remove(prefix);
        if (uri != null)
            uri2Prefix.remove(uri);
        return uri;
    }
}
