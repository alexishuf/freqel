package br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl;

import br.ufsc.lapesd.riefederator.webapis.requests.parsers.PrimitiveParser;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

@Immutable
public class DatePrimitiveParser implements PrimitiveParser {
    private static final Logger logger = LoggerFactory.getLogger(DatePrimitiveParser.class);
    private static final Pattern TIME_RX = Pattern.compile("[hHkKsSm]");
    private final @Nonnull String format;
    private final boolean hasTime;
    private final @Nonnull String isoFormat;

    public DatePrimitiveParser(@Nonnull String format) {
        if (!isValidFormat(format))
            throw new IllegalArgumentException("Invalid SimpleDateFormat: "+format);
        this.format = format;
        this.hasTime = TIME_RX.matcher(format).find();
        this.isoFormat = hasTime ? "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" : "yyyy-MM-dd";
    }

    public static boolean isValidFormat(@Nonnull String format) {
        try {
            new SimpleDateFormat(format);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static @Nullable DatePrimitiveParser tryCreate(@Nullable String format) {
        return format == null || !isValidFormat(format) ? null : new DatePrimitiveParser(format);
    }

    public @Nonnull String getFormat() {
        return format;
    }

    public boolean hasTime() {
        return hasTime;
    }

    @Override
    public @Nullable RDFNode parse(@Nullable String value) {
        if (value == null) return null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date;
        try {
            date = simpleDateFormat.parse(value.trim());
        } catch (ParseException e) {
            logger.info("Invalid date {} does not abide to format {}", value, format);
            return null;
        }
        String iso = new SimpleDateFormat(isoFormat).format(date);
        XSDDatatype dType = hasTime ? XSDDatatype.XSDdateTime : XSDDatatype.XSDdate;
        return ResourceFactory.createTypedLiteral(iso, dType);
    }

    @Override
    public String toString() {
        return "DatePrimitiveParser("+format+")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatePrimitiveParser)) return false;
        DatePrimitiveParser that = (DatePrimitiveParser) o;
        assert !getFormat().equals(that.getFormat())
                || (hasTime == that.hasTime && isoFormat.equals(that.isoFormat));
        return getFormat().equals(that.getFormat());
    }

    @Override
    public int hashCode() {
        return getFormat().hashCode();
    }
}
