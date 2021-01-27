package br.ufsc.lapesd.freqel.webapis.requests.impl;

import br.ufsc.lapesd.freqel.jena.model.term.JenaLit;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.NoTermSerializationException;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.SimpleDateSerializer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.fromJena;
import static java.util.Arrays.asList;
import static org.apache.jena.datatypes.xsd.XSDDatatype.*;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class SimpleDateSerializerTest {
    @DataProvider
    public static @Nonnull Object[][] data() {
        Lit d1 = fromJena(createTypedLiteral("2020-01-08", XSDdate));
        Lit d2 = fromJena(createTypedLiteral("2020-01-31", XSDdate));
        Lit dt1 = fromJena(createTypedLiteral("2020-01-31T13:37", XSDdateTime));
        Lit dts1 = fromJena(createTypedLiteral("2020-01-31T13:37-03:00", XSDdateTimeStamp));
        Lit dts2 = fromJena(createTypedLiteral("2020-01-31T16:37:23.666Z", XSDdateTimeStamp));

        Lit iso1 = fromJena(createTypedLiteral("2019-12-31", XSDstring));
        Lit isoAmb = fromJena(createTypedLiteral("2019-06-06", XSDstring));
        Lit isoNoZero = fromJena(createTypedLiteral("2019-6-6", XSDstring));
        Lit isoMillennium = fromJena(createTypedLiteral("08-06-06", XSDstring));
        Lit isoMillenniumNoZero = fromJena(createTypedLiteral("8-6-6", XSDstring));
        Lit isoLang = fromJena(createLangLiteral("2019-12-31", "en"));
        Lit isoTimeLang = fromJena(createLangLiteral("2019-12-31T13:47:17.666Z", "en"));
        Lit isoPlain = fromJena(createPlainLiteral("2019-12-31"));
        Lit isoTimePlain = fromJena(createPlainLiteral("2019-12-31T13:47:17.666Z"));

        JenaLit brDateAmb = fromJena(createPlainLiteral("01/02/2019"));
        JenaLit brDate = fromJena(createPlainLiteral("23/02/2019"));

        JenaLit deDateAmb = fromJena(createPlainLiteral("1.02.2019"));
        JenaLit deDate = fromJena(createPlainLiteral("23.2.2019"));

        JenaLit kpDateAmb = fromJena(createPlainLiteral("2019/10/9"));
        JenaLit kpDate = fromJena(createPlainLiteral("2019/10/23"));

        JenaLit usDateAmb = fromJena(createPlainLiteral("2/11/2020"));
        JenaLit usDate = fromJena(createPlainLiteral("2/17/2020"));

        JenaLit us2DateAmb = fromJena(createPlainLiteral("2020/2/11"));
        JenaLit us2Date = fromJena(createPlainLiteral("2020/2/17"));

        //noinspection RedundantTypeArguments (explicit type arguments speedup compilation and analysis time)
        return Stream.<List<Object>>of(
                /* parse well formed date/time literals into iso */
                asList("yyyy-MM-dd", false, d1,   "2020-01-08"),
                asList("yyyy-MM-dd", false, d2,   "2020-01-31"),
                asList("yyyy-MM-dd", false, dt1,  "2020-01-31"),
                asList("yyyy-MM-dd", false, dts1, "2020-01-31"),
                asList("yyyy-MM-dd", false, dts2, "2020-01-31"),
                /* above, but assuming american order (no effect) */
                asList("yyyy-MM-dd", true,  d1,   "2020-01-08"),
                asList("yyyy-MM-dd", true,  d2,   "2020-01-31"),
                asList("yyyy-MM-dd", true,  dt1,  "2020-01-31"),
                asList("yyyy-MM-dd", true,  dts1, "2020-01-31"),
                asList("yyyy-MM-dd", true,  dts2, "2020-01-31"),
                /* parse well formed ISO into br date format */
                asList("dd/MM/yyyy", false, d1,   "08/01/2020"),
                asList("dd/MM/yyyy", false, d2,   "31/01/2020"),
                asList("dd/MM/yyyy", false, dt1,  "31/01/2020"),
                asList("dd/MM/yyyy", false, dts1, "31/01/2020"),
                asList("dd/MM/yyyy", false, dts2, "31/01/2020"),
                /* again, assuming american order should have no effect */
                asList("dd/MM/yyyy", true,  d1,   "08/01/2020"),
                asList("dd/MM/yyyy", true,  d2,   "31/01/2020"),
                asList("dd/MM/yyyy", true,  dt1,  "31/01/2020"),
                asList("dd/MM/yyyy", true,  dts1, "31/01/2020"),
                asList("dd/MM/yyyy", true,  dts2, "31/01/2020"),
                /* parse iso-ordered dates and times from non-date literals */
                asList("yyyy-MM-dd", false, iso1,                "2019-12-31"),
                asList("yyyy-MM-dd", false, isoAmb,              "2019-06-06"),
                asList("yyyy-MM-dd", false, isoNoZero,           "2019-06-06"),
                asList("yyyy-MM-dd", false, isoMillennium,       "2008-06-06"),
                asList("yyyy-MM-dd", false, isoMillenniumNoZero, "2008-06-06"),
                asList("yyyy-MM-dd", false, isoLang,             "2019-12-31"),
                asList("yyyy-MM-dd", false, isoTimeLang,         "2019-12-31"),
                asList("yyyy-MM-dd", false, isoPlain,            "2019-12-31"),
                asList("yyyy-MM-dd", false, isoTimePlain,        "2019-12-31"),
                /* again, assuming american order should have no effect */
                asList("yyyy-MM-dd", true,  iso1,                "2019-12-31"),
                asList("yyyy-MM-dd", true,  isoAmb,              "2019-06-06"),
                asList("yyyy-MM-dd", true,  isoNoZero,           "2019-06-06"),
                asList("yyyy-MM-dd", true,  isoMillennium,       "2008-06-06"),
                asList("yyyy-MM-dd", true,  isoMillenniumNoZero, "2008-06-06"),
                asList("yyyy-MM-dd", true,  isoLang,             "2019-12-31"),
                asList("yyyy-MM-dd", true,  isoTimeLang,         "2019-12-31"),
                asList("yyyy-MM-dd", true,  isoPlain,            "2019-12-31"),
                asList("yyyy-MM-dd", true,  isoTimePlain,        "2019-12-31"),
                /* parse iso-ordered dates and times from non-date literals into br dates */
                asList("dd/MM/yyyy", false, iso1,                "31/12/2019"),
                asList("dd/MM/yyyy", false, isoAmb,              "06/06/2019"),
                asList("dd/MM/yyyy", false, isoNoZero,           "06/06/2019"),
                asList("dd/MM/yyyy", false, isoMillennium,       "06/06/2008"),
                asList("dd/MM/yyyy", false, isoMillenniumNoZero, "06/06/2008"),
                asList("dd/MM/yyyy", false, isoLang,             "31/12/2019"),
                asList("dd/MM/yyyy", false, isoTimeLang,         "31/12/2019"),
                asList("dd/MM/yyyy", false, isoPlain,            "31/12/2019"),
                asList("dd/MM/yyyy", false, isoTimePlain,        "31/12/2019"),
                /* again, assuming american order should have no effect */
                asList("dd/MM/yyyy", true,  iso1,                "31/12/2019"),
                asList("dd/MM/yyyy", true,  isoAmb,              "06/06/2019"),
                asList("dd/MM/yyyy", true,  isoNoZero,           "06/06/2019"),
                asList("dd/MM/yyyy", true,  isoMillennium,       "06/06/2008"),
                asList("dd/MM/yyyy", true,  isoMillenniumNoZero, "06/06/2008"),
                asList("dd/MM/yyyy", true,  isoLang,             "31/12/2019"),
                asList("dd/MM/yyyy", true,  isoTimeLang,         "31/12/2019"),
                asList("dd/MM/yyyy", true,  isoPlain,            "31/12/2019"),
                asList("dd/MM/yyyy", true,  isoTimePlain,        "31/12/2019"),
                /* parse varied formats into ISO dates */
                asList("yyyy-MM-dd", false, brDateAmb,           "2019-02-01"),
                asList("yyyy-MM-dd", false, brDate,              "2019-02-23"),
                asList("yyyy-MM-dd", false, deDateAmb,           "2019-02-01"),
                asList("yyyy-MM-dd", false, deDate,              "2019-02-23"),
                asList("yyyy-MM-dd", false, kpDateAmb,           "2019-10-09"),
                asList("yyyy-MM-dd", false, kpDate,              "2019-10-23"),
                asList("yyyy-MM-dd", false, usDateAmb,           "2020-11-02"),
                asList("yyyy-MM-dd", false, usDate,              "2020-02-17"),
                asList("yyyy-MM-dd", false, us2DateAmb,          "2020-02-11"),
                asList("yyyy-MM-dd", false, us2Date,             "2020-02-17"),
                /* some cases are not affect by changes in assumeAmerican */
                asList("yyyy-MM-dd", true,  brDate,              "2019-02-23"),
                asList("yyyy-MM-dd", true,  deDateAmb,           "2019-02-01"),
                asList("yyyy-MM-dd", true,  deDate,              "2019-02-23"),
                asList("yyyy-MM-dd", true,  kpDateAmb,           "2019-10-09"),
                asList("yyyy-MM-dd", true,  kpDate,              "2019-10-23"),
                asList("yyyy-MM-dd", true,  usDate,              "2020-02-17"),
                asList("yyyy-MM-dd", true,  us2DateAmb,          "2020-02-11"),
                asList("yyyy-MM-dd", true,  us2Date,             "2020-02-17"),
                /* while others are */
                asList("yyyy-MM-dd", true,  brDateAmb,           "2019-01-02"),
                asList("yyyy-MM-dd", true,  usDateAmb,           "2020-02-11")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "data")
    public void test(@Nonnull String format, boolean assumeAmerican, Term term, String expected) {
        SimpleDateSerializer serializer = new SimpleDateSerializer(format, assumeAmerican);
        boolean caught = false;
        try {
            assertEquals(serializer.toString(term, null, null), expected);
        } catch (NoTermSerializationException e) {
            caught = true;
        }
        if (expected == null)
            assertTrue(caught);
    }

}