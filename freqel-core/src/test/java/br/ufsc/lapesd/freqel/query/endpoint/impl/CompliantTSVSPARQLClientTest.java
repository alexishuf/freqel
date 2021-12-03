package br.ufsc.lapesd.freqel.query.endpoint.impl;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class CompliantTSVSPARQLClientTest {
    @DataProvider public static Object[][] escapeData() {
        return Stream.of(
                asList("", ""),
                asList("a", "a"),
                asList("0_9.*", "0_9.*"),
                asList("x?", "x%3F"),
                asList("<>", "%3C%3E"),
                asList("\n", "%0A"),
                asList("\u001F", "%1F"),
                asList("\u007F", "%7F")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "escapeData")
    public void testEscape(@Nonnull String in, @Nonnull String expected) {
        assertEquals(CompliantTSVSPARQLClient.escape(in), expected);
    }
}