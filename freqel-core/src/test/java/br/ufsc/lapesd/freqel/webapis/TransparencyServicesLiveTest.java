package br.ufsc.lapesd.freqel.webapis;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlannerTest;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecException;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.testng.Assert.*;

@Test
public class TransparencyServicesLiveTest implements TransparencyServiceTestContext {

    private @Nonnull Op loadQuery(@Nonnull String filename)
            throws IOException, SPARQLParseException {
        try (InputStream in = getClass().getResourceAsStream(filename)) {
            assertNotNull(in);
            return SPARQLParser.tolerant().parse(in);
        }
    }
    private @Nonnull Federation getBudgetFederation() throws IOException, FederationSpecException {
        File configFile = new File("examples/federations/budget-scenario.yaml");
        assertTrue(configFile.exists());
        return new FederationSpecLoader().load(configFile);
    }

    @Test(enabled = false)
    public void testProcurementsOfContractsPlan() throws Exception {
        Federation federation = getBudgetFederation();
        Op query = loadQuery("live/procurements-of-contract.sparql");
        Op plan = federation.plan(query);
        ConjunctivePlannerTest.assertPlanAnswers(plan, query);
    }

    @Test(enabled = false)
    public void testProcurementsOfContracts() throws Exception {
        Federation federation = getBudgetFederation();
        Op query = loadQuery("live/procurements-of-contract.sparql");
        Set<Term> ids = new HashSet<>();
        try (Results results = federation.query(query)) {
            assertTrue(results.hasNext());
            while (results.hasNext()) {
                Solution solution = results.next();
                Term startDate = solution.get("startDate");
                assertNotNull(startDate);
                assertEquals(startDate.asLiteral().getLexicalForm(), "2019-12-02");
                ids.add(solution.get("id"));
            }
        }
        assertEquals(ids.size(), 2);
        assertTrue(ids.stream().noneMatch(Objects::isNull));
    }
}
