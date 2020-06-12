package br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl;

import org.apache.jena.rdf.model.Literal;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDdate;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDdateTime;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class DatePrimitiveParserTest {

    @DataProvider
    public static Object[][] parseData() {
        String isoFmtStr = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
        SimpleDateFormat isoFmt = new SimpleDateFormat(isoFmtStr);
        String current = isoFmt.format(new Date());
        Matcher matcher = Pattern.compile("\\.\\d\\d\\d(.*)$").matcher(current);
        assertTrue(matcher.find());
        String timezone = matcher.group(1);

        return Stream.of(
                asList("dd/MM/yyyy", "23/03/2019", createTypedLiteral("2019-03-23", XSDdate)),
                asList("dd/MM/yyyy", " 23/03/2019 ", createTypedLiteral("2019-03-23", XSDdate)),
                asList("dd/MM/yyyy", "", null),
                asList("dd/MM/yyyy", "  ", null),
                asList("dd/MM/yyyy", "23-03-2019", null),
                asList("dd/MM/yyyy", "23.03.2019", null),
                asList("dd.MM.yyyy", "23.03.2019", createTypedLiteral("2019-03-23", XSDdate)),
                asList("MM/dd/yyyy", "03/23/2019", createTypedLiteral("2019-03-23", XSDdate)),
                asList("MM/dd/yyyy", "3/23/2019", createTypedLiteral("2019-03-23", XSDdate)),
                asList("dd/MM/yyyy HH:mm", "01/12/2020 14:03",
                        createTypedLiteral("2020-12-01T14:03:00.000"+timezone, XSDdateTime)),
                asList(isoFmtStr, "2019-12-06T14:06:01.666"+timezone,
                        createTypedLiteral("2019-12-06T14:06:01.666"+timezone, XSDdateTime))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(String format, String value, Literal expected) {
        DatePrimitiveParser parser = DatePrimitiveParser.tryCreate(format);
        assertNotNull(parser);
        assertEquals(parser.getFormat(), format);
        if (expected != null) {
            String expectedDatatypeURI = expected.getDatatype().getURI();
            assertEquals(parser.hasTime(), expectedDatatypeURI.equals(XSDdateTime.getURI()));
        }
        assertEquals(parser.parse(value), expected);
    }
}