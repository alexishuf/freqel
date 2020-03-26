package br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl;

import br.ufsc.lapesd.riefederator.webapis.requests.parsers.PrimitiveParser;
import org.apache.jena.rdf.model.RDFNode;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class PrimitiveParsersRegistryTest {

    private static class DummyParser implements PrimitiveParser, Comparable<DummyParser> {
        int id;

        public DummyParser(int id) {
            this.id = id;
        }

        @Override
        public@Nullable RDFNode parse(@Nullable String value) {
            return null;
        }

        @Override
        public @Nonnull String toString() {
            return String.format("DummyParser(%d)", id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DummyParser)) return false;
            DummyParser that = (DummyParser) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public int compareTo(@Nonnull PrimitiveParsersRegistryTest.DummyParser o) {
            return Integer.compare(id, o.id);
        }
    }

    @Test
    public void testAddOutOfOrder() {
        PrimitiveParsersRegistry registry = new PrimitiveParsersRegistry();
        registry.add(asList("a", "c"), new DummyParser(1));
        registry.add(asList("x", "y"), new DummyParser(2));
        registry.add(asList("a", "b"), new DummyParser(3));
        registry.add(asList("a", "c"), new DummyParser(4));
        assertEquals(registry.getEntries(), asList(
                new PrimitiveParsersRegistry.Entry(asList("a", "b"), new DummyParser(3)),
                new PrimitiveParsersRegistry.Entry(asList("a", "c"), new DummyParser(1)),
                new PrimitiveParsersRegistry.Entry(asList("a", "c"), new DummyParser(4)),
                new PrimitiveParsersRegistry.Entry(asList("x", "y"), new DummyParser(2))
        ));
    }

    @Test
    public void testAddDuplicate() {
        PrimitiveParsersRegistry registry = new PrimitiveParsersRegistry();
        registry.add(asList("a", "c"), new DummyParser(1));
        registry.add(asList("x", "y"), new DummyParser(2));
        registry.add(asList("a", "c"), new DummyParser(1));
        assertEquals(registry.getEntries(), asList(
                new PrimitiveParsersRegistry.Entry(asList("a", "c"), new DummyParser(1)),
                new PrimitiveParsersRegistry.Entry(asList("x", "y"), new DummyParser(2))
        ));
    }

    private @Nonnull PrimitiveParsersRegistry createRegistry() {
        PrimitiveParsersRegistry registry = new PrimitiveParsersRegistry();
        registry.add(asList("a", "c"), new DummyParser(1));
        registry.add(asList("x", "y"), new DummyParser(2));
        registry.add(asList("a", "b"), new DummyParser(3));
        registry.add(asList("a", "c"), new DummyParser(4));
        return registry;
    }

    @Test
    public void testGet() {
        PrimitiveParsersRegistry registry = createRegistry();
        assertEquals(registry.get(asList("a", "b")), new DummyParser(3));
        assertEquals(registry.get(asList("a", "b"), new PlainPrimitiveParser()), new DummyParser(3));

        assertEquals(registry.get(asList("a", "c")), new DummyParser(1));
        assertEquals(registry.get(asList("x", "y")), new DummyParser(2));
    }

    @Test
    public void testGetUnrelated() {
        PrimitiveParsersRegistry registry = createRegistry();
        assertNull(registry.get(singletonList("d")));
        PlainPrimitiveParser fallback = new PlainPrimitiveParser();
        assertEquals(registry.get(singletonList("d"), fallback), fallback);
    }

    @Test
    public void testGetPrefix() {
        PrimitiveParsersRegistry registry = createRegistry();
        assertNull(registry.get(singletonList("a")));
        PlainPrimitiveParser fallback = new PlainPrimitiveParser();
        assertEquals(registry.get(singletonList("a"), fallback), fallback);

        assertNull(registry.get(singletonList("x")));
        assertEquals(registry.get(singletonList("x"), fallback), fallback);
    }

    @Test
    public void testGetSuffix() {
        PrimitiveParsersRegistry registry = createRegistry();
        assertNull(registry.get(asList("a", "b", "c")));
        PlainPrimitiveParser fallback = new PlainPrimitiveParser();
        assertEquals(registry.get(asList("a", "b", "c"), fallback), fallback);

        assertNull(registry.get(asList("x", "y", "z")));
        assertEquals(registry.get(asList("x", "y", "z"), fallback), fallback);
    }

}