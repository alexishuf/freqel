package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class CQEndpointTest extends EndpointTestBase {
    private static final @Nonnull
    List<NamedFunction<InputStream, Fixture<CQEndpoint>>> endpoints = new ArrayList<>();

    static {
        for (NamedFunction<InputStream, Fixture<TPEndpoint>> f : TPEndpointTest.endpoints) {
            if (f.toString().startsWith("ARQEndpoint")) {
                //noinspection unchecked,rawtypes
                endpoints.add((NamedFunction) f);
            }
        }
    }

    @DataProvider
    public Object[][] fixtureFactories() {
        return endpoints.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @SuppressWarnings("SameParameterValue")
    protected void queryResourceTest(Function<InputStream, Fixture<CQEndpoint>> f,
                                     @Nonnull List<Triple> query,
                                     @Nonnull Set<Solution> ex) {
        String filename = "../rdf-2.nt";
        try (Fixture<CQEndpoint> fixture = f.apply(getClass().getResourceAsStream(filename))) {
            Set<Solution> ac = new HashSet<>();
            try (Results results = fixture.endpoint.query(query)) {
                results.forEachRemaining(ac::add);
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testTPSelect(Function<InputStream, Fixture<CQEndpoint>> f) {
        queryResourceTest(f, singletonList(new Triple(S, KNOWS, BOB)),
                newHashSet(MapSolution.build("S", ALICE),
                           MapSolution.build("S", DAVE)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveSelect(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(S, KNOWS, BOB),
                                    new Triple(S, AGE, A_AGE));
        queryResourceTest(f, query, singleton(MapSolution.build("S", ALICE)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveAsk(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(CHARLIE, TYPE, PERSON),
                                    new Triple(CHARLIE, AGE, A_AGE),
                                    new Triple(ALICE, KNOWS, BOB));
        queryResourceTest(f, query, singleton(MapSolution.EMPTY));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testNegativeConjunctiveAsk(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(CHARLIE, TYPE, PERSON), //ok
                                    new Triple(CHARLIE, AGE, A_AGE),   //ok
                                    new Triple(ALICE, KNOWS, DAVE));   //wrong
        queryResourceTest(f, query, emptySet());
    }
}