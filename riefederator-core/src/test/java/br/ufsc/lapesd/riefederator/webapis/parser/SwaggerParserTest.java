package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.molecules.AtomFilter;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeLink;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.TransparencyService;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.ModelMessageBodyWriter;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.impl.ParamPagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.PrimitiveParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.ResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.DatePrimitiveParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.MappedJsonResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.PrimitiveParsersRegistry;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.*;

public class SwaggerParserTest extends JerseyTestNg.ContainerPerClassTest implements TestContext {
    private static final String RESOURCES_BASE = "br/ufsc/lapesd/riefederator/webapis";
    private static final String ptExtYaml = RESOURCES_BASE + "/portal_transparencia-ext.yaml";

    private static final StdPlain id             = new StdPlain("id");
    private static final StdPlain dataAbertura = new StdPlain("dataAbertura");
    private static final StdPlain unidadeGestora = new StdPlain("unidadeGestora");
    private static final StdPlain orgaoVinculado = new StdPlain("orgaoVinculado");
    private static final StdPlain orgaoMaximo    = new StdPlain("orgaoMaximo");
    private static final StdPlain codigoSIAFI    = new StdPlain("codigoSIAFI");
    private static final StdPlain codigo         = new StdPlain("codigo");

    @DataProvider
    public static Object[][] resourcePathData() {
        return Stream.of(
                RESOURCES_BASE+"/nyt_books.yaml",
                RESOURCES_BASE+"/nyt_semantic.yaml",
                RESOURCES_BASE+"/portal_transparencia.json",
                RESOURCES_BASE+"/portal_transparencia-ext.yaml"
        ).map(s -> new Object[] {s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "resourcePathData", groups = {"fast"})
    public void testParse(String resourcePath) throws IOException {
        SwaggerParser parser = SwaggerParser.getFactory().fromResource(resourcePath);
        assertTrue(parser.getEndpoints().stream().noneMatch(Objects::isNull));
    }


    @Test(groups = {"fast"})
    public void testEndpoints() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        Collection<String> endpoints = parser.getEndpoints();
        assertTrue(endpoints.contains("/api-de-dados/licitacoes"));
        assertTrue(endpoints.contains("/api-de-dados/licitacoes-opt"));
        assertTrue(endpoints.contains("/api-de-dados/licitacoes/{id}"));
    }

    @Test(groups = {"fast"})
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

    @Test(groups = {"fast"})
    public void testDoNotApplyDefaultPagingStrategyOnMissingParameters() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String endpoint = "/api-de-dados/licitacoes/{id}";
        assertNull(parser.getPagingStrategy(endpoint, null));
    }

    @Test(groups = {"fast"})
    public void testGetParsedCardinality() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String ep = "/api-de-dados/licitacoes";
        assertEquals(parser.getCardinality(ep, Cardinality.UNSUPPORTED), Cardinality.lowerBound(3));

        ep = "/api-de-dados/licitacoes/{id}";
        assertEquals(parser.getCardinality(ep, Cardinality.UNSUPPORTED), Cardinality.UNSUPPORTED);
    }

    @Test(groups = {"fast"})
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

    @Test(groups = {"fast"})
    public void testDateParser() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        JsonSchemaMoleculeParser schemaParser = parser.parseSchema("/api-de-dados/licitacoes");

        PrimitiveParser global = schemaParser.getGlobalDateParser();
        assertNotNull(global);
        assertTrue(global instanceof DatePrimitiveParser);
        assertEquals(((DatePrimitiveParser) global).getFormat(),
                     "dd/MM/yyyy");

        assertSame(schemaParser.getParsersRegistry().get(singletonList("dataAbertura")), global);
    }

    @Test(groups = {"fast"})
    public void testMappedJsonResponseParserParsesDate() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        String endpoint = "/api-de-dados/licitacoes";
        JsonSchemaMoleculeParser schemaParser = parser.parseSchema(endpoint);
        ResponseParser responseParser = parser.getResponseParser(endpoint, schemaParser, null);
        assertTrue(responseParser instanceof MappedJsonResponseParser);

        MappedJsonResponseParser jsonResponseParser = (MappedJsonResponseParser) responseParser;
        PrimitiveParsersRegistry registry = jsonResponseParser.getPrimitiveParsers();
        assertNotNull(registry);
        assertTrue(registry.get(singletonList("dataAbertura")) instanceof DatePrimitiveParser);
    }

    @Test(groups = {"fast"})
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
        assertNull(parameters.getParamPath("id").getAtomFilter());
    }

    @Test(groups = {"fast"})
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
        AtomFilter filter = parameters.getParamPath("dataInicial").getAtomFilter();
        assertNotNull(filter);
        assertEquals(filter.getSPARQLFilter(), SPARQLFilter.build("FILTER($actual >= $input)"));
        assertEquals(filter.getSPARQLFilter().getFilterString(), "$actual >= $input");
        assertEquals(parameters.getParamPath("codigoOrgao").getPath(),
                asList(new StdPlain("unidadeGestora"),
                       new StdPlain("orgaoVinculado"),
                       new StdPlain("codigoSIAFI")));
    }

    @Override
    protected Application configure() {
        return new ResourceConfig().register(ModelMessageBodyWriter.class)
                                   .register(TransparencyService.class);
    }

    @Test
    public void selfTestGetProcurement() throws IOException {
        String json = target("/api-de-dados/licitacoes/277815533")
                .request(APPLICATION_JSON).get(String.class);
        DictTree tree = DictTree.load().fromJsonString(json);
        assertEquals(tree.getLong("id", 0), 277815533);
        assertEquals(tree.getPrimitive("unidadeGestora/codigo", "").toString(),
                "150232");

        assertEquals(target("/api-de-dados/licitacoes/666").request(APPLICATION_JSON)
                                                                 .get().getStatus(),
                     404);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void selfTestListProcurements() throws IOException {
        String json = target("/api-de-dados/licitacoes")
                .queryParam("dataInicial", "01/12/2019")
                .queryParam("dataFinal", "31/12/2019")
                .queryParam("codigoOrgao", "26246")
                .request(APPLICATION_JSON).get(String.class);
        List<Map<String, Object>> list;
        list = (List<Map<String, Object>>)new Gson().fromJson(json, List.class);

        assertEquals(list.size(), 2);
        List<DictTree> trees = new ArrayList<>();
        for (Map<String, Object> map : list) trees.add(DictTree.load().fromMap(map));

        assertEquals(trees.stream().map(d -> d.get("id")).collect(toSet()),
                Sets.newHashSet(267291791.0, 278614622.0));
        assertEquals(trees.stream().map(d -> d.get("licitacao/numero")).collect(toSet()),
                     Sets.newHashSet("003272019", "002472019"));
    }

    @Test
    public void testCreateEndpointGetProcurement() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        URI rootUri = target().getUri();
        assertEquals(parser.setHost(rootUri.getHost() + ":" + rootUri.getPort()),
                     "www.transparencia.gov.br");

        String endpointPath = "/api-de-dados/licitacoes/{id}";
        WebAPICQEndpoint endpoint = parser.getEndpoint(endpointPath);

        CQueryMatch match = endpoint.getMatcher().match(createQuery(
                x, unidadeGestora, y,
                y, orgaoMaximo, u,
                y, orgaoVinculado, z,
                z, codigoSIAFI, lit("26246")));
        assertTrue(match.isEmpty()); // missing required inputss !

        CQuery query = createQuery(
                x, id, lit(267291791),
                x, unidadeGestora, y,
                y, orgaoMaximo, u,
                u, codigo, v,
                y, orgaoVinculado, z,
                z, codigoSIAFI, lit("26246"));
        match = endpoint.getMatcher().match(query);
        assertEquals(match.getKnownExclusiveGroups().size(), 1);

        try (Results results = endpoint.query(query)) {
            assertTrue(results.hasNext());
            Solution solution = results.next();
            assertEquals(solution.get(v), lit("26000"));
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void testCreateEndpointListProcurements() throws IOException {
        SwaggerParser parser = SwaggerParser.FACTORY.fromResource(ptExtYaml);
        URI rootUri = target().getUri();
        assertEquals(parser.setHost(rootUri.getHost() + ":" + rootUri.getPort()),
                     "www.transparencia.gov.br");

        WebAPICQEndpoint endpoint = parser.getEndpoint("/api-de-dados/licitacoes");
        CQuery query = createQuery(
                x, dataAbertura, u, SPARQLFilter.build("?u >= \"2019-12-01\"^^xsd:date"),
                                    SPARQLFilter.build("?u <= \"2019-12-31\"^^xsd:date"),
                x, id,           v,
                x, unidadeGestora, y,
                y, orgaoVinculado, z,
                z, codigoSIAFI, lit("26246")
        );

        CQueryMatch match = endpoint.getMatcher().match(query);
        assertFalse(match.isEmpty());
        assertEquals(match.getKnownExclusiveGroups().size(), 1);
        assertEquals(match.getNonExclusiveRelevant().size(), 0);
        CQuery eg = match.getKnownExclusiveGroups().iterator().next();
        assertEquals(query.getSet(), eg.getSet());
        assertEquals(query.getModifiers(), eg.getModifiers());

        Set<String> ids = new HashSet<>();
        endpoint.query(query).forEachRemainingThenClose(s ->
                ids.add(Objects.requireNonNull(s.get(v)).asLiteral().getLexicalForm()));
        assertEquals(ids, Sets.newHashSet("267291791", "278614622"));
    }
}