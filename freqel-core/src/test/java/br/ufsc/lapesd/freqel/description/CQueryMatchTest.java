package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import com.google.common.base.Splitter;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class CQueryMatchTest implements TestContext {

    @Test
    public void testEmpty() {
        CQueryMatch m = new CQueryMatch();
        assertTrue(m.isEmpty());
        assertEquals(m.getAllRelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getNonExclusiveRelevant(), emptyList());
        assertEquals(m.getIrrelevant(CQuery.EMPTY), emptyList());
        assertEquals(m.getIrrelevant(createQuery(Alice, knows, x)),
                     singletonList(new Triple(Alice, knows, x)));
        assertEquals(Splitter.on('\n').splitToList(m.toString()).size(), 1);
    }

    @Test
    public void testRequireValidTriple() {
        List<Triple> query = asList(new Triple(Alice, knows, x), new Triple(x, type, Person));
        CQueryMatch.Builder builder = CQueryMatch.builder(CQuery.from(query));
        builder.addTriple(new Triple(Alice, knows, x));
        assertThrows(IllegalArgumentException.class,
                () -> builder.addTriple(new Triple(Alice, type, Person)));
    }

    @Test
    public void testRequireValidTripleInExclusiveGroup() {
        List<Triple> query = asList(new Triple(Alice, knows, x), new Triple(x, type, Person));
        CQueryMatch.Builder builder = CQueryMatch.builder(CQuery.from(query));
        List<Triple> badGroup = asList(new Triple(Alice, knows, x), new Triple(Alice, type, Person));
        assertThrows(IllegalArgumentException.class,
                () -> builder.addExclusiveGroup(badGroup));
    }

    @Test
    public void testRequireNonEmptyExclusiveGroup() {
        List<Triple> query = asList(new Triple(Alice, knows, x), new Triple(x, type, Person));
        CQueryMatch.Builder builder = CQueryMatch.builder(CQuery.from(query));
        assertThrows(IllegalArgumentException.class,
                () -> builder.addExclusiveGroup(emptyList()));
    }

    @Test
    public void testBuild() {
        List<Triple> query = asList(new Triple(Alice, knows, x),
                                    new Triple(x, type, Person),
                                    new Triple(x, knows, Bob),
                                    new Triple(x, knows, x));
        CQueryMatch m = CQueryMatch.builder(CQuery.from(query))
                .addExclusiveGroup(asList(new Triple(Alice, knows, x),
                                          new Triple(x, type, Person)))
                .addTriple(new Triple(x, knows, Bob)).build();
        assertFalse(m.isEmpty());
        assertEquals(m.getAllRelevant(), new HashSet<>(query.subList(0, 3)));
        assertEquals(m.getIrrelevant(CQuery.from(query)), query.subList(3, 4));
        assertEquals(m.getNonExclusiveRelevant(), query.subList(2, 3));
        assertEquals(m.getKnownExclusiveGroups(), singletonList(query.subList(0, 2)));
    }

}