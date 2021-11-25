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
public class SVChunkedEncoderTest implements TestContext {
    private CSVChunkedEncoder csvEncoder = new CSVChunkedEncoder();
    private TSVChunkedEncoder tsvEncoder = new TSVChunkedEncoder();

    @DataProvider public @Nonnull Object[][] testData() {
        return Stream.of(
    /*  0 */    asList(emptyList(), emptyList(), true, "\r\n", "\n"),
    /*  1 */    asList(emptyList(), singletonList(emptyList()), true, "\r\n\r\n", "\n\n"),
    /*  2 */    asList(singletonList("x"), emptyList(), false, "x\r\n", "?x\n"),
    /*  3 */    asList(asList("x", "y"), emptyList(), false, "x,y\r\n", "?x\t?y\n"),
    /*  4 */    asList(singletonList("x"), singletonList(singletonList(Alice)), false,
                        "x\r\nhttp://example.org/Alice\r\n", "?x\n<http://example.org/Alice>\n"),
    /*  5 */    asList(singletonList("x"), singletonList(singletonList(integer(23))), false,
                       "x\r\n23\r\n", "?x\n\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>\n"),
    /*  6 */    asList(singletonList("x"), singletonList(singletonList(lit("bob"))), false,
                       "x\r\nbob\r\n", "?x\n\"bob\"\n"),
    /*  7 */    asList(singletonList("x"), singletonList(singletonList(lit("roberto", "pt-BR"))), false,
                       "x\r\nroberto\r\n", "?x\n\"roberto\"@pt-BR\n"),
    /*  8 */    asList(singletonList("x"),
                       asList(singletonList(lit("alice")),
                              singletonList(lit("bob"))),
                       false,
                       "x\r\nalice\r\nbob\r\n",
                       "?x\n\"alice\"\n\"bob\"\n"),
    /*  9 */    asList(asList("r1", "firstName"),
                       asList(asList(Alice, lit("alice")),
                              asList(Bob, lit("bob", "pt-BR"))),
                       false,
                       "r1,firstName\r\nhttp://example.org/Alice,alice\r\nhttp://example.org/Bob,bob\r\n",
                       "?r1\t?firstName\n<http://example.org/Alice>\t\"alice\"\n<http://example.org/Bob>\t\"bob\"@pt-BR\n"),
    /* 10 */    asList(singletonList("x"),
                       asList(singletonList(lit("a,b")),
                              singletonList(lit("a\tb")),
                              singletonList(lit("a\nb")),
                              singletonList(lit("a\"b")),
                              singletonList(lit(",")),
                              singletonList(lit("\"")),
                              singletonList(lit("\"\"")),
                              singletonList(lit("\n"))),
                        false,
                        "x\r\n" +
                                "\"a,b\"\r\n" +
                                "a\tb\r\n" +
                                "\"a\nb\"\r\n" +
                                "\"a\"\"b\"\r\n" +
                                "\",\"\r\n" +
                                "\"\"\"\"\r\n" +
                                "\"\"\"\"\"\"\r\n" +
                                "\"\n\"\r\n",
                        "?x\n" +
                                "\"a,b\"\n" +
                                "\"a\\tb\"\n" +
                                "\"a\\nb\"\n" +
                                "\"a\\\"b\"\n" +
                                "\",\"\n" +
                                "\"\\\"\"\n" +
                                "\"\\\"\\\"\"\n" +
                                "\"\\n\"\n")
        ).map(List::toArray).toArray(Object[][]::new);
    }


    private void doTest(@Nonnull ChunkedEncoder encoder, @Nonnull List<String> vars,
                       @Nonnull List<List<Term>> rows, boolean isAsk, @Nonnull String expected) {
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

    @Test(dataProvider = "testData")
    public void testCSV(@Nonnull List<String> vars, @Nonnull List<List<Term>> rows,
                        boolean isAsk, @Nonnull String expectedCSV,
                        @Nonnull String ignored) {
        doTest(csvEncoder, vars, rows, isAsk, expectedCSV);
    }

    @Test(dataProvider = "testData")
    public void testTSV(@Nonnull List<String> vars, @Nonnull List<List<Term>> rows,
                        boolean isAsk, @Nonnull String ignored,
                        @Nonnull String expectedTSV) {
        doTest(tsvEncoder, vars, rows, isAsk, expectedTSV);
    }

}