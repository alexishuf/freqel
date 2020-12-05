package br.ufsc.lapesd.riefederator.util;

import br.ufsc.lapesd.riefederator.util.parse.RDFSourceAbstractTest;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class HDTUtilsTest extends RDFSourceAbstractTest {
    @DataProvider public  Object[][] saveData() {
        Model expected = loadModel("parse/iterable.ttl");
        Model expectedNoBlank = loadModel("parse/iterable-nb.ttl");
        Model expectedBoth = ModelFactory.createDefaultModel();
        expectedBoth.add(expected);
        expectedBoth.add(expectedNoBlank);

        List<List<Object>> lists = new ArrayList<>();
        lists.add(asList(expectedNoBlank,                   false, expectedNoBlank));
        lists.add(asList(expectedNoBlank,                   true,  expectedNoBlank));
        lists.add(asList(expected,                          false, expected       ));
        lists.add(asList(expected,                          true,  expected       ));
        lists.add(asList(asList(expectedNoBlank, expected), false, expectedBoth   ));
        lists.add(asList(asList(expectedNoBlank, expected), true,  expectedBoth   ));

        return lists.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "saveData")
    public void testSaveAndLoad(@Nonnull Object sources, boolean index,
                                @Nonnull Model expected) throws Exception {
        File file = createTempFile();
        HDT hdt = null;
        try {
            if (sources instanceof Collection)
                hdt = HDTUtils.saveHDT((Collection<?>) sources, file, index);
            else
                hdt = HDTUtils.saveHDT(file, index, sources);
            Model actual = ModelFactory.createDefaultModel();
            for (IteratorTripleString it = hdt.search(null, null, null); it.hasNext(); )
                actual.add(HDTUtils.toStatement(it.next()));
            assertTrue(expected.isIsomorphicWith(actual));
        } finally {
            if (hdt != null)
                hdt.close();
        }

        try (HDT hdt2 = HDTUtils.map(file)) {
            Model actual = ModelFactory.createDefaultModel();
            for (IteratorTripleString it = hdt2.search(null, null, null); it.hasNext(); )
                actual.add(HDTUtils.toStatement(it.next()));
            assertTrue(expected.isIsomorphicWith(actual));
        }

        try (HDT hdt3 = HDTUtils.mapIndexed(file)) {
            Model actual = ModelFactory.createDefaultModel();
            for (IteratorTripleString it = hdt3.search(null, null, null); it.hasNext(); )
                actual.add(HDTUtils.toStatement(it.next()));
            assertTrue(expected.isIsomorphicWith(actual));
        }
    }
}