package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.collect.Sets;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.query.CQueryContext.createQuery;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.*;

public class ProcurementsServiceTest extends JerseyTestNg.ContainerPerClassTest
        implements ProcurementServiceTestContext {

    @Override
    protected Application configure() {
        return new ResourceConfig().register(ProcurementsService.class);
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
    public void testQueryGetProcurementById() throws IOException {
        WebAPICQEndpoint ep = ProcurementsService.getProcurementsByIdClient(target());
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
        WebAPICQEndpoint ep = ProcurementsService.getProcurementsClient(target());
        Set<String> ids = new HashSet<>();
        ep.query(createQuery(
                x, id,             y,
                x, unidadeGestora, z,
                z, orgaoVinculado, w,
                w, codigoSIAFI,    lit("26246"),
                x, dataAbertura,   u,
                SPARQLFilter.build("?u >= \"2019-12-01\"^^xsd:date"),
                SPARQLFilter.build("?u <= \"2019-12-31\"^^xsd:date"))
        ).forEachRemainingThenClose(s
                -> ids.add(requireNonNull(s.get(y)).asLiteral().getLexicalForm()));

        assertEquals(ids, Sets.newHashSet("267291791", "278614622"));
    }

    @Test
    public void testQueryGetProcurementsOpt() throws IOException {
        WebAPICQEndpoint ep = ProcurementsService.getProcurementsOptClient(target());
        Set<String> ids = new HashSet<>();
        ep.query(createQuery(
                x, id, u,
                x, dataAbertura, y, SPARQLFilter.build("?y >= \"2019-12-01\"^^xsd:date"),
                x, valor,        z, SPARQLFilter.build("?z <= 20000")
        )).forEachRemainingThenClose(s
                -> ids.add(requireNonNull(s.get(u)).asLiteral().getLexicalForm()));
        assertEquals(ids, Collections.singleton("278614622"));
    }

}