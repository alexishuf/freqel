package br.ufsc.lapesd.riefederator.model.prefix;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.jena.model.prefix.PrefixMappingDict;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class PrefixDictTest implements TestContext {
    public static List<NamedSupplier<? extends MutablePrefixDict>> nativeMutable, mutable;

    static {
        nativeMutable = new ArrayList<>();
        nativeMutable.add(new NamedSupplier<>("StdPrefixDict", StdPrefixDict::new));
        nativeMutable.add(new NamedSupplier<>("PrefixMappingDict",
                () -> new PrefixMappingDict(new PrefixMappingImpl())));

        mutable = new ArrayList<>(nativeMutable);
        for (NamedSupplier<? extends MutablePrefixDict> supplier : nativeMutable) {
            mutable.add(new NamedSupplier<>(
                    "SynchronizedMutablePrefixDict("+supplier+")",
                    () -> new SynchronizedMutablePrefixDict(supplier.get())
            ));
        }
    }

    private static final @Nonnull String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private static final @Nonnull String FOAF_PREFIX = "http://xmlns.com/foaf/0.1/";
    private static final @Nonnull String TYPE = RDF_PREFIX + "type";
    private static final @Nonnull String PERSON = FOAF_PREFIX+"Person";
    private static final @Nonnull String ALICE = "http://example.org/Alice"; //ALICE < *_PREFIX
    private static final @Nonnull String BOB = "https://example.org/Alice"; //"https:" > "http:"

    @DataProvider
    public static Object[][] mutableData() {
        return mutable.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] emptyData() {
        return mutable.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    @Test(dataProvider = "emptyData")
    public void  testEmpty(Supplier<? extends PrefixDict> supplier) {
        PrefixDict d = supplier.get();
        assertTrue(d.isEmpty());
        AtomicInteger entryCount = new AtomicInteger();
        d.forEach((k, v) -> entryCount.incrementAndGet());
        assertEquals(entryCount.get(), 0);

        String uri = "htttp://example.org/ns#";
        assertFalse(d.shorten(uri).isShortened());
        assertEquals(d.shorten(uri).getLongURI(), uri);
        assertEquals(d.shorten(uri).toString(), uri);
        assertEquals(d.shorten(uri).getNamespaceEndPos(), 0);

        assertNull(d.expand("foaf:name", null));
        assertNull(d.expandPrefix("foaf", null));
        assertNull(d.shortenPrefix("http://xmlns.com/foaf/0.1/", null));
    }

    private void standardTest(@Nonnull PrefixDict d) {
        assertEquals(d.expand("rdf:type", null), TYPE);
        assertTrue(d.shorten(TYPE).isShortened());
        assertEquals(d.shorten(TYPE).toString(), "rdf:type");
        assertEquals(d.shorten(TYPE).getNamespaceEndPos(), TYPE.indexOf("#")+1);
        assertEquals(d.shorten(TYPE).getLocalName(), "type");
        assertEquals(d.shorten(TYPE).getPrefix(), "rdf");
        assertEquals(d.shorten(TYPE).getNamespace(), RDF_PREFIX);

        assertEquals(d.shortenPrefix(RDF_PREFIX, null), "rdf");
        assertEquals(d.expandPrefix("rdf", null), RDF_PREFIX);

        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> e : d.entries()) map.put(e.getKey(), e.getValue());
        assertEquals(map.get("rdf"), RDF_PREFIX);
    }

    @Test
    public void testStandard() {
        standardTest(StdPrefixDict.STANDARD);
    }

    @Test
    public void testDefault() {
        standardTest(StdPrefixDict.DEFAULT);
    }

    @Test(dataProvider = "mutableData")
    public void testAddPrefixAndExpand(Supplier<? extends MutablePrefixDict> supplier) {
        MutablePrefixDict d = supplier.get();
        assertNull(d.put("rdf", RDF_PREFIX));
        assertEquals(d.expand("rdf:type", null), TYPE);
        assertEquals(d.expandPrefix("rdf", null), RDF_PREFIX);

        assertNull(d.expand("foaf:Person", null));
        assertNull(d.expandPrefix("foaf", null));
    }

    @Test(dataProvider = "mutableData")
    public void testAddPrefixAndShorten(Supplier<? extends MutablePrefixDict> supplier) {
        MutablePrefixDict d = supplier.get();
        assertNull(d.put("foaf", FOAF_PREFIX));
        assertEquals(d.shorten(PERSON).toString(null), "foaf:Person");
        assertFalse(d.shorten(TYPE).isShortened());
    }

    @Test(dataProvider = "mutableData")
    public void testAddPrefixAndIterate(Supplier<? extends MutablePrefixDict> supplier) {
        MutablePrefixDict d = supplier.get();
        assertTrue(d.isEmpty());

        assertNull(d.put("rdf", RDF_PREFIX));
        assertNull(d.put("foaf", FOAF_PREFIX));

        Map<String, String> map = new HashMap<>();
        if (d instanceof SynchronizedMutablePrefixDict) {
                d.forEach(map::put);
        } else {
            for (Map.Entry<String, String> e : d.entries()) map.put(e.getKey(), e.getValue());
        }
        assertEquals(map.size(), 2);
        assertEquals(map.get("rdf"), RDF_PREFIX);
        assertEquals(map.get("foaf"), FOAF_PREFIX);

        Map<String, String> map2 = new HashMap<>();
        d.forEach(map2::put);
        assertEquals(map, map2);
    }

    @Test(dataProvider = "mutableData")
    public void testChangePrefix(Supplier<? extends MutablePrefixDict> supplier) {
        MutablePrefixDict d = supplier.get();
        assertNull(d.put("rdf", RDF_PREFIX));
        PrefixDict.Shortened s = d.shorten(TYPE);
        assertTrue(s.isShortened());
        assertEquals(s.toString(), "rdf:type");

        assertEquals(d.put("rdf", FOAF_PREFIX), RDF_PREFIX);
        assertEquals(d.expand("rdf:Person", null), PERSON);

        assertNull(d.put("foaf", FOAF_PREFIX));
        assertEquals(d.expand("foaf:Person", null), PERSON);

        // result is non-deterministic
        HashSet<String> expected = new HashSet<>(asList("rdf:Person", "foaf:Person"));
        expected.retainAll(Collections.singleton(d.shorten(PERSON).toString(null)));
        assertEquals(expected.size(), 1);
    }

    @Test(dataProvider = "mutableData")
    public void testNonPrefixable(Supplier<? extends MutablePrefixDict> supplier) {
        MutablePrefixDict d = supplier.get();
        d.put("rdf", RDF_PREFIX);
        for (String uri : asList(ALICE, BOB)) {
            assertFalse(d.shorten(uri).isShortened());
            assertEquals(d.shorten(uri).toString(), uri);
            assertEquals(d.shorten(uri).getNamespace(), "");
            assertNull(d.shorten(uri).getPrefix());
            assertEquals(d.shorten(uri).getLocalName(), uri);
        }
    }

    @Test(dataProvider = "mutableData")
    public void testAddAlphabet(Supplier<? extends MutablePrefixDict> supplier) {
        MutablePrefixDict d = supplier.get();
        String preamble = "http://example.org/";
        String letters = "abcdefghijklmnoprstuvxywz";
        for (int i = 0; i < letters.length(); i++)
            d.put(letters.substring(i, i + 1), preamble + letters.charAt(i) + "/");
        // cannto shorten: too short
        assertFalse(d.shorten(preamble).isShortened());
        // shorten the prefixes
        for (int i = 0; i < letters.length(); i++) {
            String uri = preamble + letters.charAt(i) + "/";
            PrefixDict.Shortened shortened = d.shorten(uri);
            assertTrue(shortened.isShortened());
            assertEquals(shortened.getNamespace(), uri);
            assertEquals(shortened.getPrefix(), letters.substring(i, i+1));
            assertEquals(shortened.getLocalName(), "");
        }
        // shorten URIs
        for (int i = 0; i < letters.length(); i++) {
            String namespace = preamble + letters.charAt(i) + "/";
            PrefixDict.Shortened shortened = d.shorten(namespace+"inst");
            assertTrue(shortened.isShortened());
            assertEquals(shortened.getNamespace(), namespace);
            assertEquals(shortened.getPrefix(), letters.substring(i, i+1));
            assertEquals(shortened.getLocalName(), "inst");
        }
    }

    @DataProvider
    public static @Nonnull Object[][] sameEntryData() {
        String exA = EX + "a/", exB = EX + "b/";
        return Stream.of(
                asList(asList("a", exA), asList("a", exA), true),
                asList(asList("b", exB), asList("a", exA), false),
                asList(asList("a", exA), asList("b", exB), false),
                asList(asList("a", exA), asList("a", exB), false),
                asList(asList("a", exB), asList("a", exA), false),
                asList(asList("a", exA), emptyList(),      false),
                asList(emptyList(),      asList("a", exA), false),

                asList(asList("a", exA), asList("a", exA, "b", exB), false),
                asList(asList("a", exA, "b", exB), asList("a", exA), false),

                asList(asList("a", exA, "b", exB), asList("a", exA, "b", exB), true),
                asList(asList("a", exA, "b", exB), asList("b", exB, "a", exA), true),
                asList(asList("b", exB, "a", exA), asList("a", exA, "b", exB), true)
        ).flatMap(l -> mutable.stream().map(s -> {
            ArrayList<Object> copy = new ArrayList<>(l);
            copy.add(0, s);
            return copy;
        })).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "sameEntryData")
    public void testSameEntryData(Supplier<? extends MutablePrefixDict> supplier,
                                  List<String> left, List<String> right, boolean expected) {
        assertEquals(left.size() % 2, 0);
        assertEquals(right.size() % 2, 0);
        MutablePrefixDict leftDict = supplier.get(), rightDict = supplier.get();
        for (int i = 0, size = left.size(); i < size; i += 2)
            leftDict.put(left.get(i), left.get(i+1));
        for (int i = 0, size = right.size(); i < size; i += 2)
            rightDict.put(right.get(i), right.get(i+1));

        assertEquals(leftDict.sameEntries(rightDict), expected);
        assertEquals(rightDict.sameEntries(leftDict), expected);
    }

}