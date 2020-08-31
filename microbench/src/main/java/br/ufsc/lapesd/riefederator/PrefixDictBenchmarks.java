package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.deprecated.TriePrefixDict;
import br.ufsc.lapesd.riefederator.jena.model.prefix.PrefixMappingDict;
import br.ufsc.lapesd.riefederator.model.prefix.MutablePrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class PrefixDictBenchmarks {
    private static final String ALPHABET = "abcdefghijklmnopqrstuvxywz";

    @Param({"empty", "standard", "default"})
    private String prefixDataset;

    @Param({"trie", "jena", "sorted"})
    private String prefixImpl;

    private List<ImmutablePair<String, String>> name2prefix;
    private PrefixDict prefixDict;
    private Function<List<ImmutablePair<String, String>>, PrefixDict> prefixDictFactory;
    private List<String> longUris, shortUris;

    private @Nonnull List<ImmutablePair<String, String>> getPrefixDataset() {
        PrefixDict in;
        if (prefixDataset.equals("empty"))
            in = StdPrefixDict.EMPTY;
        else if (prefixDataset.equals("standard"))
            in = StdPrefixDict.STANDARD;
        else if (prefixDataset.equals("default"))
            in = StdPrefixDict.DEFAULT;
        else
            throw new RuntimeException("Unknown prefixSet="+ prefixDataset);
        List<ImmutablePair<String, String>> list = new ArrayList<>();
        for (Map.Entry<String, String> e : in.entries())
            list.add(ImmutablePair.of(e.getKey(), e.getValue()));
        return list;
    }

    private @Nonnull PrefixDict getPrefixImpl(@Nullable List<ImmutablePair<String, String>> data) {
        MutablePrefixDict dict;
        if (prefixImpl.equals("trie")) {
            dict = new TriePrefixDict();
        } else if (prefixImpl.equals("jena")) {
            dict = new PrefixMappingDict(new PrefixMappingImpl());
        } else if (prefixImpl.equals("sorted")) {
            dict = new StdPrefixDict();
        } else {
            throw new RuntimeException("Bad prefixImpl="+prefixImpl);
        }
        if (data == null)
            return dict;
        for (ImmutablePair<String, String> e : data)
            dict.put(e.left, e.right);
        return dict;
    }

    private @Nonnull Function<List<ImmutablePair<String, String>>, PrefixDict>
    getPrefixDictFactory() {
        if (prefixImpl.equals("trie")) {
            return l -> {
                TriePrefixDict dict = new TriePrefixDict();
                for (ImmutablePair<String, String> p : l)
                    dict.put(p.left, p.right);
                return dict;
            };
        } else if (prefixImpl.equals("jena")) {
            return l -> {
                PrefixMappingDict dict = new PrefixMappingDict(new PrefixMappingImpl());
                for (ImmutablePair<String, String> p : l)
                    dict.put(p.left, p.right);
                return dict;
            };
        } else if (prefixImpl.equals("sorted")) {
            return l -> {
                StdPrefixDict.Builder b = StdPrefixDict.builder();
                for (ImmutablePair<String, String> p : l)
                    b.put(p.left, p.right);
                return b.build();
            };
        } else {
            throw new RuntimeException("Bad prefixImpl="+prefixImpl);
        }
    }

    @Setup(Level.Trial)
    public void setUp() {
        name2prefix = getPrefixDataset();
        Random random = new Random(78954317);
        Collections.shuffle(name2prefix, random);
        prefixDict = getPrefixImpl(name2prefix);
        prefixDictFactory = getPrefixDictFactory();
        longUris = new ArrayList<>();
        shortUris = new ArrayList<>();
        StringBuilder b = new StringBuilder(12);
        for (ImmutablePair<String, String> e : name2prefix) {
            b.setLength(0);
            for (int i = 0; i < 12; i++)
                b.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
            String local = b.toString();
            longUris.add(e.right + local);
            shortUris.add(e.left + ":" + local);
        }
    }

    @Benchmark
    public @Nonnull PrefixDict buildDict() {
        return prefixDictFactory.apply(name2prefix);
    }

    @Benchmark
    public @Nonnull List<String> expand() {
        ArrayList<String> list = new ArrayList<>(shortUris.size());
        for (String shortened : shortUris)
            list.add(prefixDict.expand(shortened, null));
        return list;
    }

    @Benchmark
    public @Nonnull List<String> shorten() {
        ArrayList<String> list = new ArrayList<>(longUris.size());
        for (String expanded : longUris)
            list.add(prefixDict.shorten(expanded).toString());
        return list;
    }

    @Benchmark
    public @Nonnull List<Map.Entry<String, String>> iterateDict() {
        List<Map.Entry<String, String>> list = new ArrayList<>(name2prefix.size());
        for (Map.Entry<String, String> e : prefixDict.entries())
            list.add(e);
        return list;
    }
}
