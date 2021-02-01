package br.ufsc.lapesd.freqel.webapis;

import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.google.common.collect.Sets;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.io.IOException;
import java.util.*;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.*;

public class TransparencyServiceTest extends JerseyTestNg.ContainerPerClassTest
        implements TransparencyServiceTestContext {

    @Override
    protected Application configure() {
        return new ResourceConfig().register(TransparencyService.class);
    }

    @Test
    public void selfTestGetProcurementById() throws IOException {
        String path = "api-de-dados/licitacoes/267291791";
        String json = target(path).request(APPLICATION_JSON).get(String.class);
        DictTree tree = DictTree.load().fromJsonString(json);
        assertEquals(tree.getLong("id", 0), 267291791);
        assertEquals(tree.getLong("situacaoCompra/codigo", 0), 75);
    }

    @Test
    public void selfTestGetProcurements() throws IOException {
        String path = "api-de-dados/licitacoes";
        String json = target(path)
                .queryParam("dataInicial", "01/12/2019")
                .queryParam("dataFinal", "31/12/2019")
                .queryParam("codigoOrgao", "26246")
                .request(APPLICATION_JSON).get(String.class);
        List<DictTree> trees = DictTree.load().fromJsonStringList(json);
        assertEquals(trees.size(), 2);
        Set<Long> ids = trees.stream().map(d -> d.getLong("id", 0)).collect(toSet());
        assertEquals(ids, Sets.newHashSet(267291791L, 278614622L));
    }

    @Test
    public void selfTestGetProcurementsOpt() throws IOException {
        String path = "api-de-dados/licitacoes-opt";
        String json = target(path)
                .queryParam("dataInicial", "01/12/2019")
                .queryParam("dataFinal", "31/12/2019")
                .queryParam("maxValor", "25000")
                .queryParam("codigoOrgao", "26246")
                .request(APPLICATION_JSON).get(String.class);
        List<DictTree> list = DictTree.load().fromJsonStringList(json);
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getLong("id", 0), 278614622L);
    }

    @Test
    public void selfTestGetContractById() throws IOException {
        String path = "/api-de-dados/contratos/id";
        String json = target(path).queryParam("id", "70507179")
                .request(APPLICATION_JSON).get(String.class);
        DictTree contract = DictTree.load().fromJsonString(json);
        assertEquals(contract.getString("id"), "70507179");
        assertEquals(contract.getDouble("valorInicialCompra"), 33780.0);
    }

    @Test
    public void selfTestGetContracts() throws IOException {
        String path = "/api-de-dados/contratos";
        String jsonString = target(path)
                .queryParam("dataInicial", "01/12/2019")
                .queryParam("dataFinal", "02/12/2020")
                .queryParam("codigoOrgao", 26246)
                .queryParam("pagina", 1).request(APPLICATION_JSON).get(String.class);
        List<DictTree> list = DictTree.load().fromJsonStringList(jsonString);
        assertEquals(list.size(), 2);
        Set<String> ids = list.stream().map(t -> t.getString("id")).collect(toSet());
        assertEquals(ids, Sets.newHashSet("70507179", "71407155"));
    }

    @Test
    public void selfTestGetProcurementByNumber() throws IOException {
        String path = "/api-de-dados/licitacoes/por-uasg-modalidade-numero";
        String jsonString = target(path)
                .queryParam("codigoUASG", "153163")
                .queryParam("numero", "001792019")
                .queryParam("codigoModalidade", -99)
                .request(APPLICATION_JSON).get(String.class);
        DictTree tree = DictTree.load().fromJsonString(jsonString);
        assertEquals(tree.getString("id"), "301718143");
    }

    @Test
    public void selfTestGetOrgaosSiafi() throws IOException {
        String path = "/api-de-dados/orgaos-siafi";
        String university = "Universidade Federal de Santa Catarina";

        String jsonString = target(path).queryParam("codigo", "26246")
                .queryParam("pagina", 1).request(APPLICATION_JSON)
                .get(String.class);
        List<DictTree> list1 = DictTree.load().fromJsonStringList(jsonString);
        assertEquals(list1.size(), 1);
        assertEquals(list1.get(0).getString("descricao"), university);

        jsonString = target(path).queryParam("descricao", university)
                .queryParam("pagina", 1).request(APPLICATION_JSON)
                .get(String.class);
        List<DictTree> list2 = DictTree.load().fromJsonStringList(jsonString);
        assertEquals(list2.size(), 1);
        assertEquals(list2.get(0).asMap(), list1.get(0).asMap());

        jsonString = target(path).queryParam("descricao", university)
                .queryParam("pagina", 2).request(APPLICATION_JSON)
                .get(String.class);
        List<DictTree> list3 = DictTree.load().fromJsonStringList(jsonString);
        assertEquals(list3.size(), 0);
    }

    @Test
    public void testQueryGetProcurementById() throws IOException {
        WebAPICQEndpoint ep = TransparencyService.getProcurementsByIdClient(target());
        Results results = ep.query(createQuery(
                x, id, lit(267291791),
                x, dataAbertura, u,
                x, valor, v
        ));
        assertTrue(results.hasNext());
        Solution next = results.next();
        assertEquals(next.get(v), lit(100878));
        assertEquals(next.get(u), date("2019-12-11"));
        assertFalse(results.hasNext());
    }

    @Test
    public void testQueryGetProcurements() throws IOException {
        WebAPICQEndpoint ep = TransparencyService.getProcurementsClient(target());
        Set<String> ids = new HashSet<>();
        ep.query(createQuery(
                x, id,             y,
                x, unidadeGestora, z,
                z, orgaoVinculado, w,
                w, codigoSIAFI,    lit("26246"),
                x, dataAbertura,   u,
                JenaSPARQLFilter.build("?u >= \"2019-12-01\"^^xsd:date"),
                JenaSPARQLFilter.build("?u <= \"2019-12-31\"^^xsd:date"))
        ).forEachRemainingThenClose(s
                -> ids.add(requireNonNull(s.get(y)).asLiteral().getLexicalForm()));

        assertEquals(ids, Sets.newHashSet("267291791", "278614622"));
    }

    @Test
    public void testQueryGetProcurementsOpt() throws IOException {
        WebAPICQEndpoint ep = TransparencyService.getProcurementsOptClient(target());
        Set<String> ids = new HashSet<>();
        ep.query(createQuery(
                x, id, u,
                x, dataAbertura, y, JenaSPARQLFilter.build("?y >= \"2019-12-01\"^^xsd:date"),
                x, valor,        z, JenaSPARQLFilter.build("?z <= 20000")
        )).forEachRemainingThenClose(s
                -> ids.add(requireNonNull(s.get(u)).asLiteral().getLexicalForm()));
        assertEquals(ids, Collections.singleton("278614622"));
    }

    @Test
    public void testGetProcurementByNumber() throws IOException {
        WebAPICQEndpoint ep = TransparencyService.getProcurementByNumberClient(target());
        Set<String> ids = new HashSet<>();
        ep.query(createQuery(
                x, unidadeGestora, x1, x1, codigo, lit("153163"),
                x, licitacao, x2, x2, numero, lit("001792019"), // insert missing 2 leading zeros
                x, modalidadeLicitacao, x3, x3, codigo, lit(-99),
                x, id, y)
        ).forEachRemainingThenClose(s -> ids.add(requireNonNull(s.get(y)).asLiteral().getLexicalForm()));
        assertEquals(ids, Sets.newHashSet("301718143"));
    }

    @Test
    public void testGetContractById() throws IOException {
        WebAPICQEndpoint ep = TransparencyService.getContractByIdClient(target());
        List<Lit> values = new ArrayList<>();
        ep.query(createQuery(x, id, lit(75507145), x, valorInicialCompra, y))
                .forEachRemainingThenClose(s -> values.add(requireNonNull(s.get(y)).asLiteral()));
        assertEquals(values, singletonList(lit(29000)));
    }

    @Test
    public void testGetContracts() throws IOException {
        WebAPICQEndpoint ep = TransparencyService.getContractsClient(target());
        Set<Lit> values = new HashSet<>();
        ep.query(createQuery(
                x, dataInicioVigencia, x1,
                JenaSPARQLFilter.build("?x1 >= \"2019-12-01\"^^xsd:date"),
                x, dataFimVigencia, x2,
                JenaSPARQLFilter.build("?x2 <= \"2020-12-02\"^^xsd:date"),
                x, unidadeGestora, x3, x3, orgaoVinculado, x4, x4, codigoSIAFI, lit("26246"),
                x, valorInicialCompra, v)
        ).forEachRemainingThenClose(s -> values.add(requireNonNull(s.get(v)).asLiteral()));
        assertEquals(values, Sets.newHashSet(lit(33780), lit(88733.34)));
    }

    @Test
    public void testGetOrgaosSiafi() throws IOException {
        String name = "Universidade Federal de Santa Catarina";
        WebAPICQEndpoint ep = TransparencyService.getOrgaosSiafiClient(target());
        Set<Lit> codes = new HashSet<>();
        ep.query(createQuery(
                x, descricao, lit(name),
                x, codigo, y)
        ).forEachRemainingThenClose(s -> codes.add(requireNonNull(s.get(y)).asLiteral()));
        assertEquals(codes, singleton(lit("26246")));

        Set<Lit> names = new HashSet<>();
        ep.query(createQuery(
                x, descricao, y,
                x, codigo, lit("26246"))
        ).forEachRemainingThenClose(s -> names.add(requireNonNull(s.get(y)).asLiteral()));
        assertEquals(names, singleton(lit(name)));
    }
}