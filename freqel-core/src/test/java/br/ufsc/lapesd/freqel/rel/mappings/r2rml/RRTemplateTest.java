package br.ufsc.lapesd.freqel.rel.mappings.r2rml;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.RRFactory;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.RRTemplateException;
import br.ufsc.lapesd.freqel.rel.sql.impl.NaturalSqlTermParser;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class RRTemplateTest implements TestContext {

    @BeforeClass
    public void classSetUp() {
        RRFactory.install();
    }

    @DataProvider
    public static @Nonnull Object[][] parseData() {
        return Stream.of(
                asList("{A}", singletonList("A")),
                asList("{A}-{B}", asList("A", "B")),
                asList("http://example.org/{A}", singletonList("A")),
                asList("http://example.org/{A\\{2\\}}", singletonList("A{2}")),
                asList("http://example.org/{A}/{B}", asList("A", "B")),
                asList("http://example.org/{A}/{B}/{C}", asList("A", "B", "C")),
                asList("http://example.org/{A}/{B}?c={C}", asList("A", "B", "C")),
                asList("http://example.org/{A}/{\"B\"}/{C}", asList("A", "\"B\"", "C")),
                asList("http://example.org/{A}/{'B'}/{C}", asList("A", "'B'", "C")),
                asList("http://example.org/{A}/{\\{B\\}}/{C}", asList("A", "{B}", "C")),
                asList("http://example.org/{A}/{-\\{B\\}-}/{C}", asList("A", "-{B}-", "C"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(String string, List<String> columns) {
        if (columns == null) {
            expectThrows(RRTemplateException.class, () -> new RRTemplate(string));
        } else {
            RRTemplate template = new RRTemplate(string);
            assertEquals(template.getTemplate(), string);
            assertEquals(template.getColumnNames(), new HashSet<>(columns));
            assertEquals(template.getOrderedColumnNames(), columns);
        }
    }

    @DataProvider
    public static @Nonnull Object[][] expandStringData() {
        return Stream.of(
                asList("http://example.org/{A}", TermType.IRI, "http://example.org/A"),
                asList("http://{host}.org/{A}", TermType.IRI, "http://host.org/A"),
                asList("http://example.org/{A?}", TermType.IRI, "http://example.org/A%3F"),
                asList("http://example.org/{A\\}}", TermType.IRI, "http://example.org/A%7D"),
                asList("http://example.org/{A}/{B}", TermType.IRI, "http://example.org/A/B"),
                asList("http://example.org/{A\\}}/{B?}", TermType.IRI, "http://example.org/A%7D/B%3F"),
                asList("{A}", TermType.Literal, "A"),
                asList("{A}-{B}", TermType.Literal, "A-B"),
                asList("{A} {B}", TermType.Literal, "A B"),
                asList("{A}?{B}", TermType.Literal, "A?B"),
                asList("{A\\}}?{B}", TermType.Literal, "A}?B"),
                asList("{A\\}}?{B/}", TermType.Literal, "A}?B/")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "expandStringData")
    public void testExpandString(@Nonnull String string, @Nonnull TermType type,
                                 @Nonnull String expected) {
        RRTemplate template = new RRTemplate(string);
        Map<String, Object> map = new HashMap<>();
        for (String columnName : template.getColumnNames())
            map.put(columnName, columnName.replaceAll("\\?[{}]", ""));
        assertEquals(template.tryExpandToString(map, type, NaturalSqlTermParser.INSTANCE), expected);
    }

    @DataProvider
    public static Object[][] tryExpandData() {
        Map<String, Object> m1 = new HashMap<>(), m2 = new HashMap<>(), m3 = new HashMap<>();
        m1.put("A", "a");
        m2.put("A", "a?");
        m3.put("A", createTypedLiteral(2));
        m3.put("B", createLangLiteral("b", "en"));
        Map<String, Object> m4 = new HashMap<>();
        m4.put("A", Alice);

        return Stream.of(
                asList(EX+"{A}",      m1, "tm-iri", createResource(EX+"a")),
                asList(EX+"{A}",      m2, "tm-iri", createResource(EX+"a%3F")),
                asList(EX+"{A}/{B}",  m3, "tm-iri", createResource(EX+"2/b")),
                asList(EX+"?uri={A}", m4, "tm-iri",
                       createResource(EX+"?uri=http%3A%2F%2Fexample.org%2FAlice")),
                // Similar to the previous 4, but generates a string literal
                asList("{A}",      m1, "tm-string", createTypedLiteral("a")),
                asList("{A}",      m2, "tm-string", createTypedLiteral("a?")),
                asList("{A}/{B}",  m3, "tm-string", createTypedLiteral("2/b")),
                asList("{A}",      m4, "tm-string",
                        createTypedLiteral("http://example.org/Alice")),
                // above 4 test cases generating a string literal
                asList(EX+"{A}",      m1, "tm-string", createTypedLiteral(EX+"a")),
                asList(EX+"{A}",      m2, "tm-string", createTypedLiteral(EX+"a?")),
                asList(EX+"{A}/{B}",  m3, "tm-string", createTypedLiteral(EX+"2/b")),
                asList(EX+"?uri={A}", m4, "tm-string",
                       createTypedLiteral(EX+"?uri=http://example.org/Alice")),
                // above 4 test cases generating a plain (defaults to xsd:string) literal
                asList(EX+"{A}",      m1, "tm-string", createTypedLiteral(EX+"a")),
                asList(EX+"{A}",      m2, "tm-string", createTypedLiteral(EX+"a?")),
                asList(EX+"{A}/{B}",  m3, "tm-string", createTypedLiteral(EX+"2/b")),
                asList(EX+"?uri={A}", m4, "tm-string",
                       createTypedLiteral(EX+"?uri=http://example.org/Alice")),
                // above 4 test cases generating a lang literal
                asList(EX+"{A}",      m1, "tm-lang", createLangLiteral(EX+"a", "en")),
                asList(EX+"{A}",      m2, "tm-lang", createLangLiteral(EX+"a?", "en")),
                asList(EX+"{A}/{B}",  m3, "tm-lang", createLangLiteral(EX+"2/b", "en")),
                asList(EX+"?uri={A}", m4, "tm-lang",
                       createLangLiteral(EX+"?uri=http://example.org/Alice", "en"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "tryExpandData")
    public void testTryExpand(@Nonnull String string, @Nonnull Map<String, Object> assignments,
                              @Nonnull String termMapLocalName,
                              @Nonnull RDFNode expected) throws IOException {
        TermMap termMap;
        try (InputStream in = getClass().getResourceAsStream("term-maps.ttl")) {
            assertNotNull(in);
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, in, Lang.TTL);
            termMap = model.createResource(EX + termMapLocalName).as(TermMap.class);
        }

        RRTemplate template = new RRTemplate(string);
        RDFNode actual = template.tryExpand(assignments, termMap);
        assertEquals(actual, expected);
        assertEquals(template.expand(assignments, termMap), expected);
    }

}