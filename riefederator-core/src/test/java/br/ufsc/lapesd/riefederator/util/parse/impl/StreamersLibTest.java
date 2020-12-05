package br.ufsc.lapesd.riefederator.util.parse.impl;

import br.ufsc.lapesd.riefederator.util.parse.RDFSourceAbstractTest;
import br.ufsc.lapesd.riefederator.util.parse.RDFStreamer;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.sparql.graph.GraphFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class StreamersLibTest extends RDFSourceAbstractTest {

    @DataProvider public Object[][] streamData() {
        String res = "../iterable.ttl";
        Model expected = loadModel(res);

        StreamersLib.FileStreamer fileStreamer = new StreamersLib.FileStreamer();
        StreamersLib.URIStreamer uriStreamer = new StreamersLib.URIStreamer();
        StreamersLib.StringStreamer strStreamer = new StreamersLib.StringStreamer();
        List<List<Object>> lists = allFormats()
                .flatMap(fmt -> Stream.of(
                        asList(fileStreamer, extract(res, fmt), expected),
                        asList(fileStreamer, extractSuffixed(res, fmt), expected),
                        asList(uriStreamer, extract(res, fmt).toURI(), expected),
                        asList(uriStreamer, extractSuffixed(res, fmt).toURI(), expected),
                        asList(strStreamer, extract(res, fmt).getAbsolutePath(), expected),
                        asList(strStreamer, extractSuffixed(res, fmt).getAbsolutePath(), expected))
                ).collect(Collectors.toList());

        return lists.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "streamData")
    public void testStream(@Nonnull RDFStreamer streamer, @Nonnull Object source,
                           @Nullable Model expected) {
        assertEquals(streamer.canStream(source), expected != null);
        if (expected != null) {
            Graph graph = GraphFactory.createDefaultGraph();
            streamer.stream(source, new StreamRDFBase() {
                @Override public void triple(Triple triple) {
                    graph.add(triple);
                }
            });
            Model actual = ModelFactory.createModelForGraph(graph);
            assertTrue(expected.isIsomorphicWith(actual));
        }
    }
}