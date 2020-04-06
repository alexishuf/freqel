package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.ProjectingResults;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class ProjectingResultsTest {
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
        CollectionResults results = new CollectionResults(emptyList(), newHashSet("x", "y", "z"));
        expectThrows(IllegalArgumentException.class,
                () -> new ProjectingResults(results, singleton("w")));
    }

    @Test
    public void testRetainsCardinality() {
        CollectionResults empty = new CollectionResults(emptyList(), singleton("x"));
        CollectionResults two = new CollectionResults(Arrays.asList(
                MapSolution.build("x", uri(1)),
                MapSolution.build("x", uri(2))
        ), singleton("x"));

        ProjectingResults p = new ProjectingResults(empty, singleton("x"));
        assertEquals(p.getCardinality(), Cardinality.exact(0));

        p = new ProjectingResults(two, singleton("x"));
        assertEquals(p.getCardinality(), Cardinality.exact(2));
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