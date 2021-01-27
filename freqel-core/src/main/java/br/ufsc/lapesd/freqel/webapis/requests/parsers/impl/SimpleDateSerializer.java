package br.ufsc.lapesd.freqel.webapis.requests.parsers.impl;

import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.TermSerializer;
import com.google.common.collect.Sets;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;

public class SimpleDateSerializer implements TermSerializer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleDateFormat.class);
    private static final Pattern DATE_RX = Pattern.compile(
            "(-?\\d\\d?\\d?\\d?)([-./])(\\d\\d?)([-./])(-?\\d\\d?\\d?\\d?)");
    private static final Set<RDFDatatype> dateDatatypes = Sets.newHashSet(XSDDatatype.XSDdate,
            XSDDatatype.XSDdateTime, XSDDatatype.XSDdateTimeStamp);

    public static final @Nonnull
    SimpleDateSerializer INSTANCE
            = new SimpleDateSerializer(new SimpleDateFormat("yyyy-MM-dd"));

    private final boolean assumeAmericanInputs;
    private final @Nonnull SimpleDateFormat resultFormat;

    public SimpleDateSerializer(@Nonnull String resultFormat) {
        this(new SimpleDateFormat(resultFormat), false);
    }
    public SimpleDateSerializer(@Nonnull SimpleDateFormat resultFormat) {
        this(resultFormat, false);
    }
    public SimpleDateSerializer(@Nonnull String resultFormat,
                                boolean assumeAmericanInputs) {
        this(new SimpleDateFormat(resultFormat), assumeAmericanInputs);
    }
    public SimpleDateSerializer(@Nonnull SimpleDateFormat resultFormat,
                                boolean assumeAmericanInputs) {
        this.assumeAmericanInputs = assumeAmericanInputs;
        this.resultFormat = resultFormat;
    }

    /**
     * Indicates whether format is a valid format or not.
     */
    public static boolean isValidFormat(@Nonnull String format) {
        try {
            new SimpleDateFormat(format);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }


    public @Nonnull SimpleDateFormat getResultFormat() {
        return resultFormat;
    }

    public boolean getAssumeAmericanInputs() {
        return assumeAmericanInputs;
    }

    @Override
    public String toString() {
        return String.format("SimpleDateFormatSerializer(%s%s)", resultFormat.toPattern(),
                assumeAmericanInputs ? ", assumeAmericanInputs" : "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleDateSerializer)) return false;
        SimpleDateSerializer that = (SimpleDateSerializer) o;
        return getAssumeAmericanInputs() == that.getAssumeAmericanInputs() &&
                getResultFormat().equals(that.getResultFormat());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAssumeAmericanInputs(), getResultFormat());
    }

    @Override
    public @Nonnull String toString(@Nonnull Term term, @Nullable String paramName,
                                    @Nullable APIRequestExecutor executor)
                throws NoTermSerializationException {
        Date date = null;
        if (term.isLiteral()) {
            Literal lit = JenaWrappers.toJena((Lit) term);
            if (dateDatatypes.contains(lit.getDatatype())) {
                Matcher matcher = DATE_RX.matcher(lit.getLexicalForm());
                if (matcher.find()) {
                    int y = parseInt(matcher.group(1)), m = parseInt(matcher.group(3)),
                        d = parseInt(matcher.group(5));
                    if (m == 0) {
                        logger.warn("Bad date: {}. Assuming month=01", lit);
                        m = 1;
                    }
                    //noinspection MagicConstant
                    date = new GregorianCalendar(y, m-1, d).getTime();
                }
            } else {
                String lex = lit.getLexicalForm();
                Matcher matcher = DATE_RX.matcher(lex);
                while (matcher.find()) {
                    String[] s = {matcher.group(1), matcher.group(3), matcher.group(5)};
                    int[] v = {parseInt(s[0]), parseInt(s[1]), parseInt(s[2])};

                    int over31 = (v[0]>31?1:0) + (v[1]>31?1:0) + (v[2]>31?1:0);
                    if (over31 > 1) {
                        logger.debug("Date candidate {} has more than one component > 31", lex);
                        continue; //invalid
                    }
                    if (v[1] > 31) {
                        logger.debug("No date format has year in the middle position: {}", lex);
                        continue; //invalid
                    }

                    //start with ISO 8601 ordering
                    int[] dmy = {2, 1, 0};
                    if (matcher.group(2).equals("-")) {
                        if (v[2] > 31) {
                            if (assumeAmericanInputs)
                                dmy = new int[]{1, 0, 2}; // M-d-y
                            else
                                dmy = new int[]{0, 1, 2}; // d-M-y
                        }
                    }
                    if (matcher.group(2).equals(".")) {
                        dmy = new int[]{0, 1, 2}; // d.M.y
                        if (v[0] > 31)
                            dmy = new int[]{2, 1, 0}; // y.M.d
                    } else if (matcher.group(2).equals("/")) {
                        if (v[0] > 31)
                            dmy = new int[]{2, 1, 0}; // y/M/d
                        else if (assumeAmericanInputs)
                            dmy = new int[]{1, 0, 2}; // M/d/y
                        else
                            dmy = new int[]{0, 1, 2}; // d/M/y
                    }
                    if (v[dmy[1]] > 12) { // month > 12: swap month & day
                        if (v[dmy[0]] > 12) {
                            logger.debug("Both month and day fragments are > 12 in {}", lex);
                            continue; // swapping would not solve, try next match
                        }
                        int tmp = dmy[0];
                        dmy[0] = dmy[1];
                        dmy[1] = tmp;
                    }

                    if (s[dmy[2]].length() <= 2 && v[dmy[2]] >= 0) {
                        int decade = v[dmy[2]];
                        if (decade < 50)
                            v[dmy[2]] = 2000 + decade;
                        else
                            v[dmy[2]] = 2000 - decade;
                    }

                    int month = v[dmy[1]];
                    if (month == 0) {
                        logger.warn("Bad date: {}. Assuming month=1", lex);
                        month = 1;
                    }
                    //noinspection MagicConstant
                    date = new GregorianCalendar(v[dmy[2]], month-1, v[dmy[0]]).getTime();
                }
            }
        }
        if (date == null)
            throw new NoTermSerializationException(term, "Could parse "+term+"as a date");
        return resultFormat.format(date);
    }
}
