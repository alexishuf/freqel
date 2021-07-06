package br.ufsc.lapesd.freqel.rel.mappings.r2rml.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.PredicateObjectMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.RRFactory;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermMap;
import br.ufsc.lapesd.freqel.rel.sql.impl.NaturalSqlTermParser;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class TermContextTest implements TestContext {

    private final NaturalSqlTermParser PARSER;
    private Model model;

    public TermContextTest() {
        PARSER = NaturalSqlTermParser.INSTANCE;
    }

    @BeforeClass
    public void setUp() throws IOException {
        RRFactory.install();
        try (InputStream in = open("rel/mappings/r2rml/impl/term-context-test.ttl")) {
            assertNotNull(in);
            model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, in, Lang.TTL);
        }
    }

    private TermMap loadTermMap(String localName) {
        TermMap termMap = null;
        if (localName.startsWith("shortcut")) {
            PredicateObjectMap po = model.createResource(EX + localName)
                    .as(PredicateObjectMap.class);
            if (localName.startsWith("shortcut-predicate"))
                termMap = po.getPredicateMaps().iterator().next();
            else if (localName.startsWith("shortcut-object"))
                termMap = po.getObjectMaps().iterator().next();
            else
                fail("Unexpected localName: "+localName);
        } else {
            termMap = model.createResource(EX + localName).as(TermMap.class);
        }

        return termMap;
    }

    @DataProvider
    public static Object[][] getColumnNamesData() {
        return Stream.of(
                asList("column-plain", singletonList("A")),
                asList("column-lang", singletonList("A")),
                asList("const-iri", emptyList()),
                asList("shortcut-predicate", emptyList()),
                asList("shortcut-object", emptyList()),
                asList("shortcut-object-lit", emptyList()),
                asList("tpl-iri", asList("A", "B")),
                asList("tpl-lit", asList("A", "B")),
                asList("ref-obj", asList("A", "B")),
                asList("ref-swap", asList("B", "A"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "getColumnNamesData")
    public void testGetColumnNames(String localName, List<String> expected) {
        TermContext ctx = new TermContext(loadTermMap(localName));
        assertEquals(ctx.getColumnNames(), expected);
    }

    @DataProvider
    public static Object[][] createTermData() {
        Map<String, Object>  a2Alice = new HashMap<>();
        a2Alice.put("A", Alice);
        Map<String, Object>  strings = new HashMap<>();
        strings.put("A", "a");
        strings.put("B", "b");
        Map<String, Object>  literals = new HashMap<>();
        literals.put("A", ResourceFactory.createLangLiteral("a", "en"));
        literals.put("B", createTypedLiteral(23));

        return Stream.of(
                asList("column-plain", strings, createTypedLiteral("a")),
                asList("column-plain", literals, createLangLiteral("a", "en")),
                asList("column-plain", a2Alice, createTypedLiteral(Alice.getURI())),
                asList("column-lang", literals, createLangLiteral("a", "en")),
                asList("column-lang", strings, createLangLiteral("a", "en")),
                asList("column-lang", a2Alice, createLangLiteral(Alice.getURI(), "en")),
                asList("const-iri", strings, JenaWrappers.toJena(Alice)),
                asList("shortcut-predicate", strings, JenaWrappers.toJena(knows)),
                asList("shortcut-object", strings, JenaWrappers.toJena(Bob)),
                asList("shortcut-object-lit", strings, createTypedLiteral(23)),
                asList("shortcut-object-lit", literals, createTypedLiteral(23)),
                asList("tpl-iri", strings, createResource(EX+"a/b")),
                asList("tpl-iri", literals, createResource(EX+"a/23")),
                asList("tpl-iri", a2Alice, null),
                asList("tpl-lit", strings, createTypedLiteral("a - b")),
                asList("tpl-lit", literals, createTypedLiteral("a - 23")),
                asList("ref-obj",  strings,  createResource(EX+"a/b")),
                asList("ref-obj",  literals, createResource(EX+"a/23")),
                asList("ref-swap", strings,  createResource(EX+"b/a")),
                asList("ref-swap", literals, createResource(EX+"23/a"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "createTermData")
    public void testCreateTerm(String localName, Map<String, Object> map, RDFNode expected) {
        TermContext ctx = new TermContext(loadTermMap(localName));
        assertEquals(ctx.createTerm(map, PARSER), expected);
    }
}