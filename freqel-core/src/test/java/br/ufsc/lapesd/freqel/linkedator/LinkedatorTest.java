package br.ufsc.lapesd.freqel.linkedator;

import br.ufsc.lapesd.freqel.federation.Source;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.std.StdPlain;
import br.ufsc.lapesd.freqel.model.term.std.TemplateLink;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.webapis.TransparencyService;
import br.ufsc.lapesd.freqel.webapis.TransparencyServiceTestContext;
import br.ufsc.lapesd.freqel.webapis.WebAPICQEndpoint;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.jena.vocabulary.OWL2;
import org.testng.annotations.Test;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class LinkedatorTest implements TransparencyServiceTestContext {

    @Test
    public void testWriteAndLoadResult() throws IOException {
        StdPrefixDict prefixDict = new StdPrefixDict();
        prefixDict.put("", StdPlain.URI_PREFIX);
        prefixDict.put("owl", OWL2.NS);
        CQuery templateQuery = createQuery(prefixDict, x, id, u, y, id, u);
        TemplateLink template = new TemplateLink(OWL2.sameAs.getURI(), templateQuery, x, y);
        LinkedatorResult result = new LinkedatorResult(template, 0.95);

        Linkedator linkedator = new Linkedator();
        StringBuilderWriter stringBuilderWriter = new StringBuilderWriter();
        linkedator.writeLinkedatorResults(stringBuilderWriter, singleton(result));
        String yamlString = stringBuilderWriter.toString();

        List<DictTree> trees = DictTree.load().fromYamlList(new StringReader(yamlString));
        Collection<LinkedatorResult> parsed = linkedator.parseLinkedatorResults(trees);

        assertEquals(parsed, singleton(result));
    }

    @Test
    public void testFindWriteAndLoadTransparencyService() throws IOException {
        WebTarget target = ClientBuilder.newClient().target("http://example.org");
        WebAPICQEndpoint contracts = TransparencyService.getContractsClient(target);
        WebAPICQEndpoint contractsById = TransparencyService.getContractByIdClient(target);
        WebAPICQEndpoint procurements = TransparencyService.getProcurementsClient(target);
        WebAPICQEndpoint procurementsByNumber = TransparencyService.getProcurementByNumberClient(target);
        WebAPICQEndpoint procurementsById = TransparencyService.getProcurementsByIdClient(target);
        List<Source> sources = Stream.of(
                contracts, contractsById, procurements, procurementsById, procurementsByNumber
        ).map(WebAPICQEndpoint::asSource).collect(Collectors.toList());

        Linkedator linkedator = Linkedator.getDefault();
        List<LinkedatorResult> suggestions = linkedator.getSuggestions(sources);
        assertTrue(suggestions.size() >= 2,
                   "Expected suggestions.size() >= 2, got "+suggestions.size());
        StringBuilderWriter stringBuilderWriter = new StringBuilderWriter();
        linkedator.writeLinkedatorResults(stringBuilderWriter, suggestions);

        String yamlString = stringBuilderWriter.toString();
        List<DictTree> dictTrees = DictTree.load().fromYamlList(new StringReader(yamlString));
        Collection<LinkedatorResult> parsed = linkedator.parseLinkedatorResults(dictTrees);

        assertEquals(new HashSet<>(suggestions), new HashSet<>(parsed));
    }

}