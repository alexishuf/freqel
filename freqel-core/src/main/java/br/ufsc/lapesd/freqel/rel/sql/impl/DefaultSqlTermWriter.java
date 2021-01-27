package br.ufsc.lapesd.freqel.rel.sql.impl;

import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.rel.common.RelationalTermWriter;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;

public class DefaultSqlTermWriter implements RelationalTermWriter {
    public static final @Nonnull
    RelationalTermWriter INSTANCE = new DefaultSqlTermWriter();

    @Override
    public @Nullable String apply(@Nonnull Term term) {
        if (term.isVar())
            return null;
        if (term.isBlank())
            return null;
        if (term.isURI())
            return "'"+term.asURI().getURI()+"'";
        if (term.isLiteral()) {
            Literal lit = JenaWrappers.toJena(term.asLiteral());
            RDFDatatype dt = lit.getDatatype();
            if (dt.equals(XSDDatatype.XSDdate)) {
                return "'"+lit.getLexicalForm()+"'";
            } else if (dt.equals(XSDDatatype.XSDdateTime)) {
                return "'"+lit.getLexicalForm().replace("T", " ")+"'";
            }
            Class<?> cls = lit.getDatatype().getJavaClass();
            if (cls != null && Boolean.class.isAssignableFrom(cls)) {
                return lit.getBoolean() ? "1" : "0";
            } else if (cls != null && BigInteger.class.isAssignableFrom(cls)) {
                return lit.getValue().toString();
            } else if (cls != null && BigDecimal.class.isAssignableFrom(cls)) {
                return lit.getValue().toString();
            } else if (cls != null && (Long.class.isAssignableFrom(cls)
                                    || Integer.class.isAssignableFrom(cls))) {
                return String.valueOf(lit.getLong());
            } else if (cls != null && (Double.class.isAssignableFrom(cls)
                                    || Float.class.isAssignableFrom(cls))) {
                return String.format("%f", lit.getDouble());
            } else {
                return "'"+lit.getLexicalForm()+"'";
            }
        }
        return null;
    }
}
