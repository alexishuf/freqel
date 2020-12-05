package br.ufsc.lapesd.riefederator.util.parse.impl;

import br.ufsc.lapesd.riefederator.util.HDTUtils;
import br.ufsc.lapesd.riefederator.util.parse.JenaTripleIteratorFactory;
import br.ufsc.lapesd.riefederator.util.parse.RDFSourceAbstractTest;
import br.ufsc.lapesd.riefederator.util.parse.iterators.JenaTripleIterator;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.graph.GraphFactory;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.TripleWriter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class JenaTripleIteratorFactoriesLibTest extends RDFSourceAbstractTest {

    @DataProvider public  Object[][] iterateData() throws Exception {
        String res = "../iterable.ttl";
        Model expected = loadModel(res);
        File hdtFile = createTempFile(".hdt");
        HDTSpecification spec = new HDTSpecification();
        try (FileOutputStream out = new FileOutputStream(hdtFile);
             TripleWriter writer = HDTManager.getHDTWriter(out, "urn:relative:", spec)) {
            for (StmtIterator it = expected.listStatements(); it.hasNext(); )
                writer.addTriple(HDTUtils.toTripleString(it.next()));
        }
        File ttlFile = extract(res, RDFFormat.TTL);
        HDT hdt = HDTManager.generateHDT(ttlFile.getAbsolutePath(), "urn:relative:",
                                         RDFNotation.TURTLE, spec, HDTUtils.NULL_LISTENER);

        JenaTripleIteratorFactory modelFac = new JenaTripleIteratorFactoriesLib.ModelFactory();
        JenaTripleIteratorFactory graphFac = new JenaTripleIteratorFactoriesLib.GraphFactory();
        JenaTripleIteratorFactory hdtFac   = new JenaTripleIteratorFactoriesLib.HDTFactory();
        JenaTripleIteratorFactory fileFac  = new JenaTripleIteratorFactoriesLib.HDTFileFactory();
        return Stream.of(
                asList(loadModel(res), modelFac, expected),
                asList(loadModel(res), hdtFac, null),
                asList(loadModel(res).getGraph(), graphFac, expected),
                asList(hdtFile, fileFac, expected),
                asList(hdt, hdtFac, expected)
        ).map(List::toArray).toArray(Object[][]::new);

    }

    @Test(dataProvider = "iterateData")
    public void testIterate(@Nonnull Object source, @Nonnull JenaTripleIteratorFactory factory,
                            @Nullable Model expected) {
        assertEquals(factory.canCreate(source), expected != null);
        if (expected != null) {
            Graph graph = GraphFactory.createDefaultGraph();
            try (JenaTripleIterator it = factory.create(source)) {
                while (it.hasNext())
                    graph.add(it.next());
            }
            Model actual = ModelFactory.createModelForGraph(graph);
            assertTrue(expected.isIsomorphicWith(actual));
        }
    }

}