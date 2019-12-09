package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import com.google.common.base.Splitter;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class CQueryMatchTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI PERSON = new StdURI(FOAF.Person.getURI());
    public static final @Nonnull StdVar X = new StdVar("X");


    @Test
    public void testEmpty() {
        CQueryMatch m = new CQueryMatch(emptyList(), emptyList(), emptyList());
        assertTrue(m.isEmpty());
        assertEquals(m.getAllRelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getNonExclusiveRelevant(), emptyList());
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(Splitter.on('\n').splitToList(m.toString()).size(), 1);
    }

    @Test
    public void testRequireValidTriple() {
        List<Triple> query = asList(new Triple(ALICE, KNOWS, X), new Triple(X, TYPE, PERSON));
        CQueryMatch.Builder builder = CQueryMatch.builder(query);
        builder.addTriple(new Triple(ALICE, KNOWS, X));
        assertThrows(IllegalArgumentException.class,
                () -> builder.addTriple(new Triple(ALICE, TYPE, PERSON)));
    }

    @Test
    public void testRequireValidTripleInExclusiveGroup() {
        List<Triple> query = asList(new Triple(ALICE, KNOWS, X), new Triple(X, TYPE, PERSON));
        CQueryMatch.Builder builder = CQueryMatch.builder(query);
        List<Triple> badGroup = asList(new Triple(ALICE, KNOWS, X), new Triple(ALICE, TYPE, PERSON));
        assertThrows(IllegalArgumentException.class,
                () -> builder.addExclusiveGroup(badGroup));
    }

    @Test
    public void testRequireNonEmptyExclusiveGroup() {
        List<Triple> query = asList(new Triple(ALICE, KNOWS, X), new Triple(X, TYPE, PERSON));
        CQueryMatch.Builder builder = CQueryMatch.builder(query);
        assertThrows(IllegalArgumentException.class,
                () -> builder.addExclusiveGroup(emptyList()));
    }

    @Test
    public void testBuild() {
        List<Triple> query = asList(new Triple(ALICE, KNOWS, X),
                                    new Triple(X, TYPE, PERSON),
                                    new Triple(X, KNOWS, BOB),
                                    new Triple(X, KNOWS, X));
        CQueryMatch m = CQueryMatch.builder(query)
                .addExclusiveGroup(asList(new Triple(ALICE, KNOWS, X),
                                          new Triple(X, TYPE, PERSON)))
                .addTriple(new Triple(X, KNOWS, BOB)).build();
        assertFalse(m.isEmpty());
        assertEquals(m.getAllRelevant(), query.subList(0, 3));
        assertEquals(m.getIrrelevant(), query.subList(3, 4));
        assertEquals(m.getNonExclusiveRelevant(), query.subList(2, 3));
        assertEquals(m.getKnownExclusiveGroups(), singletonList(query.subList(0, 2)));
    }

}