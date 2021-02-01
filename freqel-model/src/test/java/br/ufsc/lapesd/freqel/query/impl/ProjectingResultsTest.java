package br.ufsc.lapesd.freqel.query.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.query.results.impl.ProjectingResults;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;

import static br.ufsc.lapesd.freqel.ResultsAssert.assertExpectedResults;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(groups = {"fast"})
public class ProjectingResultsTest implements TestContext {
    private @Nonnull StdURI uri(int i) {
        return new StdURI("http://example.oth/"+i);
    }

    @Test
    public void testVarNames() {
        CollectionResults results = new CollectionResults(emptyList(), newHashSet("x", "y", "z"));
        ProjectingResults project = new ProjectingResults(results, singleton("y"));
        assertEquals(project.getVarNames(), singleton("y"));
    }

    @Test
    public void testBadVarName() {
        ArrayList<Solution> list = new ArrayList<>();
        list.add(MapSolution.builder().put("x", Alice)
                                      .put("y", Bob)
                                      .put("z", Charlie).build());
        CollectionResults results = new CollectionResults(list, newHashSet("x", "y", "z"));
        ProjectingResults project = new ProjectingResults(results, singleton("w"));
        assertEquals(project.getVarNames(), singleton("w"));
        assertExpectedResults(project, singletonList(MapSolution.build("w", null)));
    }

    @Test
    public void testGoodAndBadVarName() {
        ArrayList<Solution> list = new ArrayList<>();
        list.add(MapSolution.builder().put("x", Alice)
                                      .put("y", Bob)
                                      .put("z", Charlie).build());
        CollectionResults results = new CollectionResults(list, newHashSet("x", "y", "z"));
        ProjectingResults project = new ProjectingResults(results, newHashSet("x", "w"));
        assertEquals(project.getVarNames(), newHashSet("x", "w"));

        assertExpectedResults(project,
                singletonList(MapSolution.builder().put("x", Alice)
                                                   .put("w", null).build()));
    }

    @Test
    public void testConsume() {
        MapSolution s1 = MapSolution.builder().put("x", uri(1))
                                                    .put("y", uri(2)).build();
        CollectionResults one = new CollectionResults(singleton(s1), newHashSet("x", "y"));

        ProjectingResults p = new ProjectingResults(one, singleton("x"));
        assertEquals(p.next(), MapSolution.build("x", uri(1)));
        assertFalse(p.hasNext());
    }
}