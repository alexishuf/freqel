package br.ufsc.lapesd.riefederator.model.prefix;

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
    public boolean isEmpty() {
        return prefix2URI.isEmpty();
    }

    @Override
    public synchronized  @Nonnull Shortened shorten(@Nonnull String uri) {
        // TODO remove synchronized from here after replacing PatriciaTrie (lookups may cause a remove followed by add)
        SortedMap<String, String> map = uri2Prefix.headMap(uri);
        String key, prefix = null;
        if (map.isEmpty()) {
            prefix = uri2Prefix.get(key = uri); //uri matches entirelly to a prefix
        } else {
            //if a prefix of uri is mapped, the longest will be the lastKey()
            if (uri.startsWith(key = map.lastKey()))
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
