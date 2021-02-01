package br.ufsc.lapesd.freqel.webapis.requests.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdBlank;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.NoTermSerializationException;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.OnlyNumbersTermSerializer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class OnlyNumbersTermSerializerTest implements TestContext {
    private static final StdLit cnpjFormatado = StdLit.fromUnescaped("04.196.645/0001-00", xsdString);
    private static final StdLit cnpjFormatadoNome = StdLit.fromUnescaped("04.196.645/0001-00 MATRIZ", xsdString);
    private static final StdLit cnpjFormatadoNomeLang = StdLit.fromUnescaped("04.196.645/0001-00 MATRIZ", "pt");
    private static final StdLit cnpjWithLeading = StdLit.fromUnescaped("04196645000100", xsdInt);
    private static final StdLit cnpj = StdLit.fromUnescaped("4196645000100", xsdInt);
    private static final StdBlank blank = new StdBlank();

    private static final String expectedCnpjWithZero = "04196645000100";
    private static final String expectedCnpj = "4196645000100";
    private static final String expectedRoot   = "04196645";
    private static final String expectedOffice = "000100";

    @DataProvider
    public static Object[][] data() {
        OnlyNumbersTermSerializer def, full, root, office;
        def = OnlyNumbersTermSerializer.INSTANCE;
        full = OnlyNumbersTermSerializer.builder().setFill('0').setWidth(14).build();
        root = OnlyNumbersTermSerializer.builder().setFill('0').setWidth(14).setSlice(8).build();
        office = OnlyNumbersTermSerializer.builder().setFill('0').setWidth(14).setSlice(-6).build();
        return Stream.of(
                /* default */
                asList(def,    cnpjFormatado,         expectedCnpjWithZero),
                asList(def,    cnpjFormatadoNome,     expectedCnpjWithZero),
                asList(def,    cnpjFormatadoNomeLang, expectedCnpjWithZero),
                asList(def,    cnpjWithLeading,       expectedCnpjWithZero),
                asList(def,    cnpj,                  expectedCnpj),
                asList(def,    Alice2 ,               "2"),
                /* fixed width */
                asList(full,   cnpjFormatado,         expectedCnpjWithZero),
                asList(full,   cnpjFormatadoNome,     expectedCnpjWithZero),
                asList(full,   cnpjFormatadoNomeLang, expectedCnpjWithZero),
                asList(full,   cnpjWithLeading ,      expectedCnpjWithZero),
                asList(full,   cnpj,                  expectedCnpjWithZero),
                asList(full,   Alice2 ,               "00000000000002"),
                /* root only */
                asList(root,   cnpjFormatado,         expectedRoot),
                asList(root,   cnpjFormatadoNome,     expectedRoot),
                asList(root,   cnpjFormatadoNomeLang, expectedRoot),
                asList(root,   cnpjWithLeading,       expectedRoot),
                asList(root,   cnpj,                  expectedRoot),
                asList(root,   Alice2 ,               "00000000"),
                /* office only */
                asList(office, cnpjFormatado,         expectedOffice),
                asList(office, cnpjFormatadoNome,     expectedOffice),
                asList(office, cnpjFormatadoNomeLang, expectedOffice),
                asList(office, cnpjWithLeading,       expectedOffice),
                asList(office, cnpj,                  expectedOffice),
                asList(office, Alice2 ,               "000002"),
                /* exceptions */
                asList(def,    x,     null),
                asList(def,    blank, null),
                asList(full,   x,     null),
                asList(full,   blank, null),
                asList(root,   x,     null),
                asList(root,   blank, null),
                asList(office, x,     null),
                asList(office, blank, null),
                /* without numbers */
                asList(def, Alice, ""),
                asList(full, Alice, "00000000000000"),
                asList(root, Alice, "00000000"),
                asList(office, Alice, "000000")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "data")
    public void test(OnlyNumbersTermSerializer serializer, Term term, String expected) {
        boolean caught = false;
        try {
            assertEquals(serializer.toString(term, "x", null), expected);
        } catch (NoTermSerializationException e) {
            caught = true;
        }
        if (expected == null)
            assertTrue(caught);
    }

}