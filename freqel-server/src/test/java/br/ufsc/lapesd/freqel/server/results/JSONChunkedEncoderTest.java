package br.ufsc.lapesd.freqel.server.results;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import io.netty.buffer.ByteBuf;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class JSONChunkedEncoderTest implements TestContext {
    private final JSONChunkedEncoder encoder = new JSONChunkedEncoder();

    @DataProvider public @Nonnull Object[][] testData() {
        return Stream.of(
    /* 0 */     asList(emptyList(), emptyList(), true,
                       "{\"head\":{},\"boolean\":false}"),
    /* 1 */     asList(emptyList(), singletonList(emptyList()), true,
                       "{\"head\":{},\"boolean\":true}"),
    /* 2 */     asList(singletonList("x"), emptyList(), false,
                        "{\"head\":{\"vars\":[\"x\"]},\"results\":{\"bindings\":[]}}"),
    /* 3 */     asList(singletonList("x"), singletonList(singletonList(Alice)), false,
                        "{\"head\":{\"vars\":[\"x\"]},\"results\":{\"bindings\":[" +
                                "{\"x\":{\"type\":\"uri\",\"value\":\"http://example.org/Alice\"}}" +
                                "]}}"),
    /* 4 */     asList(singletonList("x"),
                       asList(singletonList(Alice),
                              singletonList(integer(23)),
                              singletonList(lit("bob")),
                              singletonList(lit("roberto", "pt-BR"))),
                        false,
                        "{\"head\":{\"vars\":[\"x\"]},\"results\":{\"bindings\":[" +
                                "{\"x\":{\"type\":\"uri\",\"value\":\"http://example.org/Alice\"}}," +
                                "{\"x\":{\"type\":\"literal\",\"value\":\"23\",\"datatype\":\"http://www.w3.org/2001/XMLSchema#integer\"}}," +
                                "{\"x\":{\"type\":\"literal\",\"value\":\"bob\"}}," +
                                "{\"x\":{\"type\":\"literal\",\"value\":\"roberto\",\"xml:lang\":\"pt-BR\"}}" +
                                "]}}"),
    /* 5 */     asList(asList("r1", "firstName"),
                       asList(asList(Alice, lit("alice")),
                              asList(Bob, lit("roberto", "pt-BR"))),
                       false,
                       "{\"head\":{\"vars\":[\"r1\",\"firstName\"]},\"results\":{\"bindings\":[" +
                               "{\"r1\":{\"type\":\"uri\",\"value\":\"http://example.org/Alice\"}," +
                                "\"firstName\":{\"type\":\"literal\",\"value\":\"alice\"}}," +
                               "{\"r1\":{\"type\":\"uri\",\"value\":\"http://example.org/Bob\"}," +
                                "\"firstName\":{\"type\":\"literal\",\"value\":\"roberto\",\"xml:lang\":\"pt-BR\"}}" +
                               "]}}")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull List<String> vars, @Nonnull List<List<Term>> rows, boolean isAsk,
                     @Nonnull String expected) {
        CollectionResults results = new CollectionResults(
                rows.stream().map(terms -> ArraySolution.forVars(vars).fromValues(terms))
                             .collect(Collectors.toList()),
                vars);
        List<ByteBuf> list = Flux.from(encoder.encode(DEFAULT, results, isAsk, UTF_8))
                                 .collectList().block();
        assertNotNull(list);
        if (!isAsk && !rows.isEmpty())
            assertTrue(list.size() > 1, "Not chunking!");
        String actual = list.stream().map(bb -> bb.toString(UTF_8))
                                     .reduce(String::concat).orElse(null);
        assertEquals(actual, expected);
    }

}