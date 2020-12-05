package br.ufsc.lapesd.riefederator.util.parse;

import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.riefederator.util.parse.iterators.JenaTripleIterator;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.graph.GraphFactory;
import org.rdfhdt.hdt.hdt.HDT;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.jena.riot.RDFFormat.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class RDFIterationDispatcherTest extends RDFSourceAbstractTest {

    @DataProvider public  Object[][] sourcesData() throws Exception {
        List<List<Object>> lists = new ArrayList<>();
        Model expected = loadModel("iterable.ttl");
        Model expectedNoBlank = loadModel("iterable-nb.ttl");
        lists.add(asList(loadModel("iterable.ttl"), expected));
        lists.add(asList(asList(expected, expected), expected));
        lists.add(asList(loadModel("iterable.ttl").getGraph(), expected));
        for (RDFFormat fmt : asList(TURTLE_FLAT, TURTLE_PRETTY, NT, JSONLD, RDFXML)) {
            File file = extract("iterable.ttl", fmt);
            lists.add(asList(file, expected));
            lists.add(asList(file.getAbsolutePath(), expected));
            lists.add(asList(file.toURI(), expected));
            lists.add(asList(file.toURI().toString(), expected));
            lists.add(asList(file.toURI().toASCIIString(), expected));
            lists.add(asList("file://"+file.getAbsolutePath().replaceAll(" ", "%20"), expected));
            lists.add(asList(new RDFInputStream(file), expected));

            File fileNB = extract("iterable-nb.ttl", fmt);
            lists.add(asList(asList(fileNB, fileNB), expectedNoBlank));
            lists.add(asList(asList(fileNB.toURI(), fileNB.toURI()), expectedNoBlank));
            lists.add(asList(asList(fileNB.toURI().toString(), fileNB.toURI().toString()), expectedNoBlank));
            lists.add(asList(asList(fileNB.getAbsolutePath(), fileNB.getAbsolutePath()), expectedNoBlank));
            lists.add(asList(asList(new RDFInputStream(fileNB), new RDFInputStream(fileNB)), expectedNoBlank));
        }

        HDT hdt = createHDT(expected);
        File hdtFile = createTempFile();
        saveHDT(hdtFile, hdt).close();
        lists.add(asList(hdt, expected));
        lists.add(asList(hdtFile, expected));
        lists.add(asList(new RDFInputStream(hdtFile), expected));

        Model expectedTBox = new TBoxSpec().fetchOwlImports(true)
                                           .addResource(getClass(), "iterable-onto.ttl")
                                           .loadModel();
        TBoxSpec spec = new TBoxSpec().fetchOwlImports(true)
                                      .addResource(getClass(), "iterable-onto.ttl");
        lists.add(asList(spec, expectedTBox));

        return lists.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "sourcesData")
    public void testIterate(@Nonnull Object source, @Nonnull Model expected) {
        Graph graph = GraphFactory.createDefaultGraph();
        try (JenaTripleIterator it = RDFIterationDispatcher.get().parse(source)) {
            while (it.hasNext())
                graph.add(it.next());
        }
        Model actual = ModelFactory.createModelForGraph(graph);
        assertTrue(expected.isIsomorphicWith(actual));
    }

    @Test(dataProvider = "sourcesData")
    public void testEstimate(@Nonnull Object source, @Nonnull Model expected) {
        RDFIterationDispatcher dispatcher = RDFIterationDispatcher.get();
        if (source instanceof Collection) {
            Collection<?> collection = (Collection<?>) source;
            TriplesEstimate estimate = dispatcher.estimateAll(collection);
            assertEquals(estimate.getTotalSources(), collection.size());
            if (estimate.getEstimate() == -1)
                assertEquals(estimate.getIgnoredSources(), estimate.getTotalSources());
            else
                assertTrue(estimate.getEstimate() >= collection.size()*expected.size());
        } else {
            long estimate = dispatcher.estimate(source);
            assertTrue(estimate >= -1);
            if (estimate != -1)
                assertTrue(estimate >= expected.size());
        }
    }

}