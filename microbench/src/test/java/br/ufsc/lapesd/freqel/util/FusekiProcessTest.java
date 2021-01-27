package br.ufsc.lapesd.freqel.util;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;
import org.testng.annotations.Test;

import static org.apache.jena.query.QueryExecutionFactory.sparqlService;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.testng.Assert.*;

public class FusekiProcessTest {
    private static final String EX = "http://example.org/ns#";

    @Test(invocationCount = 4, threadPoolSize = 4)
    public void test() throws Exception {
        Model model = ModelFactory.createDefaultModel();
        Resource ex0 = createResource(EX + 0);
        Resource ex1 = createResource(EX + 0);
        model.add(ex0, RDFS.seeAlso, ex1);

        String epURI;
        String query = "SELECT * WHERE { ?s ?p ?o . }";
        try (FusekiProcess process = new FusekiProcess(model)) {
            epURI = process.getSparqlEndpoint();
            try (QueryExecution execution = sparqlService(epURI, query)) {
                ResultSet resultSet = execution.execSelect();
                assertTrue(resultSet.hasNext());
                QuerySolution solution = resultSet.next();
                assertEquals(solution.get("s"), ex0);
                assertEquals(solution.get("p"), RDFS.seeAlso);
                assertEquals(solution.get("o"), ex1);
            }
        }
        assertNotNull(epURI);

        try (QueryExecution execution = sparqlService(epURI, query)) {
            execution.execSelect();
            fail("Server is still up!");
        } catch (Exception ignored) { }
    }

    @Test(invocationCount = 4, threadPoolSize = 4)
    public void testStress() throws Exception {
        Model model = ModelFactory.createDefaultModel();
        Resource ex0 = createResource(EX + 0);
        Resource ex1 = createResource(EX + 0);
        model.add(ex0, RDFS.seeAlso, ex1);

        String epURI;
        String query = "SELECT * WHERE { ?s ?p ?o . }";
        try (FusekiProcess process = new FusekiProcess(model)) {
            epURI = process.getSparqlEndpoint();
            for (int i = 0; i < 512; i++) {
                try (QueryExecution execution = sparqlService(epURI, query)) {
                    ResultSet resultSet = execution.execSelect();
                    assertTrue(resultSet.hasNext());
                    QuerySolution solution = resultSet.next();
                    assertEquals(solution.get("s"), ex0);
                    assertEquals(solution.get("p"), RDFS.seeAlso);
                    assertEquals(solution.get("o"), ex1);
                }
            }
        }
        assertNotNull(epURI);

        try (QueryExecution execution = sparqlService(epURI, query)) {
            execution.execSelect();
            fail("Server is still up!");
        } catch (Exception ignored) { }
    }

}