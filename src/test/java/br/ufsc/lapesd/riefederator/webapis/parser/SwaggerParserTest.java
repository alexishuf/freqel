package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeLink;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.impl.ParamPagingStrategy;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.*;

public class SwaggerParserTest {
    private static final String RESOURCES_BASE = "br/ufsc/lapesd/riefederator/webapis";
    private static final String ptExtYaml = RESOURCES_BASE + "/portal_transparencia-ext.yaml";

    @DataProvider
    public static Object[][] resourcePathData() {
        return Stream.of(
                RESOURCES_BASE+"/nyt_books.yaml",
                RESOURCES_BASE+"/nyt_semantic.yaml",
                RESOURCES_BASE+"/portal_transparencia.json",
                RESOURCES_BASE+"/portal_transparencia-ext.yaml"
        ).map(s -> new Object[] {s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "resourcePathData")
    public void testParse(String resourcePath) throws IOException {
        SwaggerParser parser = SwaggerParser.getFactory().fromResource(resourcePath);
        assertTrue(parser.getEndpoints().stream().noneMatch(Objects::isNull));
    }


    @Test
    public void testEndpoints() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        Collection<String> endpoints = parser.getEndpoints();
        assertTrue(endpoints.contains("/api-de-dados/licitacoes"));
        assertTrue(endpoints.contains("/api-de-dados/licitacoes/{id}"));
    }

    @Test
    public void testDefaultPagingStrategy() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String endpoint = "/api-de-dados/licitacoes";
        PagingStrategy strategy = parser.getPagingStrategy(endpoint, null);
        assertNotNull(strategy);
        assertEquals(strategy.getParametersUsed(), singletonList("pagina"));

        APIDescriptionContext context = new APIDescriptionContext();
        ParamPagingStrategy overrideStrategy = ParamPagingStrategy.build("pagina");
        context.setPagingStrategy(Pattern.compile("/api-de-dados/.*"), overrideStrategy);
        strategy = parser.getPagingStrategy(endpoint, context);
        assertNotNull(strategy);
        assertSame(strategy, overrideStrategy);

        parser.getFallbackContext().setPagingStrategy(Pattern.compile("/api-de-dados/.*"),
                                                      overrideStrategy);
        strategy = parser.getPagingStrategy(endpoint, null);
        assertNotNull(strategy);
        assertNotSame(strategy, overrideStrategy);
        assertEquals(strategy.getParametersUsed(), singletonList("pagina"));
    }

    @Test
    public void testDoNotApplyDefaultPagingStrategyOnMissingParameters() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String endpoint = "/api-de-dados/licitacoes/{id}";
        assertNull(parser.getPagingStrategy(endpoint, null));
    }

    @Test
    public void testGetParsedCardinality() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String ep = "/api-de-dados/licitacoes";
        assertEquals(parser.getCardinality(ep, Cardinality.UNSUPPORTED), Cardinality.lowerBound(3));

        ep = "/api-de-dados/licitacoes/{id}";
        assertEquals(parser.getCardinality(ep, Cardinality.UNSUPPORTED), Cardinality.UNSUPPORTED);
    }

    @Test
    public void testGetMolecule() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        Molecule molecule = parser.getMolecule("/api-de-dados/licitacoes");

        assertEquals(molecule.getCore().getOut().size(), 12);
        StdPlain valor = new StdPlain("valor");
        MoleculeLink link = molecule.getCore().getOut().stream()
                .filter(l -> l.getEdge().equals(valor)).findFirst().orElse(null);
        assertNotNull(link);
        assertTrue(link.getAtom().getName().contains("valor"));
    }

    @Test
    public void testGetParametersLicitacaoById() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String endpoint = "/api-de-dados/licitacoes/{id}";
        SwaggerParser.Parameters parameters = parser.getParameters(endpoint, null);

        assertNotNull(parameters.parser);

        assertNull(parameters.pagingStrategy); //no paging parameter
        assertEquals(parameters.paramObjMap.keySet(), Collections.singleton("id"));
        assertEquals(parameters.parameterPathMap.keySet(), Collections.singleton("id"));
        assertEquals(parameters.optional, emptySet());
        assertEquals(parameters.required, singleton("id"));

        assertEquals(parameters.getParamObj("id").getPrimitive("in", ""), "path");
        assertFalse(parameters.getParamPath("id").isIn());
        assertEquals(parameters.getParamPath("id").getPath(),
                     singletonList(new StdPlain("id")));
        assertNull(parameters.getParamPath("id").getSparqlFilter());
    }

    @Test
    public void testGetParametersLicitacoes() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String endpoint = "/api-de-dados/licitacoes";
        SwaggerParser.Parameters parameters = parser.getParameters(endpoint, null);
        assertNotNull(parameters.parser);

        assertEquals(parameters.optional, emptySet());
        assertEquals(parameters.required,
                     newHashSet("dataInicial", "dataFinal", "codigoOrgao"));
        assertTrue(parameters.pagingStrategy instanceof ParamPagingStrategy);
        assertEquals(parameters.pagingStrategy.getParametersUsed(), singletonList("pagina"));

        for (String name : parameters.required) {
            DictTree obj = parameters.getParamObj(name);
            assertEquals(obj.getPrimitive("name", "").toString(), name);
            assertTrue(obj.contains("required", true));
        }
        assertTrue(parameters.paramObjMap.containsKey("pagina"));
        assertEquals(parameters.paramObjMap.size(), 4);

        assertEquals(parameters.parameterPathMap.keySet(),
                     newHashSet("dataInicial", "dataFinal", "codigoOrgao"));
        assertEquals(parameters.getParamPath("dataInicial").getSparqlFilter(),
                     "FILTER($actual >= $input)");
        assertEquals(parameters.getParamPath("codigoOrgao").getPath(),
                asList(new StdPlain("unidadeGestora"),
                       new StdPlain("orgaoVinculado"),
                       new StdPlain("codigoSIAFI")));
    }
}