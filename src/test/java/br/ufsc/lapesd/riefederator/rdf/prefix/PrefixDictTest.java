package br.ufsc.lapesd.riefederator.rdf.prefix;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.rdf.jena.prefix.PrefixMappingDict;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class PrefixDictTest {
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

    private final @Nonnull String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private final @Nonnull String FOAF_PREFIX = "http://xmlns.com/foaf/0.1/";
    private final @Nonnull String TYPE = RDF_PREFIX + "type";
    private final @Nonnull String PERSON = FOAF_PREFIX+"Person";

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
        assertFalse(d.entries().iterator().hasNext());
        String uri = "htttp://example.org/ns#";
        assertFalse(d.shorten(uri).isShortened());
        assertEquals(d.shorten(uri).getLongURI(), uri);
        assertEquals(d.shorten(uri).toString(), uri);
        assertEquals(d.shorten(uri).getPrefixEndPos(), 0);

        assertNull(d.expand("foaf:name", null));
        assertNull(d.expandPrefix("foaf", null));
        assertNull(d.shortenPrefix("http://xmlns.com/foaf/0.1/", null));
    }

    @Test
    public void testStandard() {
        PrefixDict d = StdPrefixDict.STANDARD;

        assertEquals(d.expand("rdf:type", null), TYPE);
        assertTrue(d.shorten(TYPE).isShortened());
        assertEquals(d.shorten(TYPE).toString(), "rdf:type");
        assertEquals(d.shorten(TYPE).getPrefixEndPos(), TYPE.indexOf("#")+1);
        assertEquals(d.shorten(TYPE).getLocalName(), "type");
        assertEquals(d.shorten(TYPE).getPrefix(), "rdf");
        assertEquals(d.shorten(TYPE).getNamespace(), RDF_PREFIX);

        assertEquals(d.shortenPrefix(RDF_PREFIX, null), "rdf");
        assertEquals(d.expandPrefix("rdf", null), RDF_PREFIX);

        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> e : d.entries()) map.put(e.getKey(), e.getValue());
        assertEquals(map.get("rdf"), RDF_PREFIX);
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
        assertFalse(d.entries().iterator().hasNext());

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
}