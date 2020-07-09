package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Term;
import org.apache.jena.rdf.model.Literal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;

public class DefaultSqlTermWriter implements SqlTermWriter {
    public static final @Nonnull SqlTermWriter INSTANCE = new DefaultSqlTermWriter();

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
            Class<?> cls = lit.getDatatype().getJavaClass();
            if (Boolean.class.isAssignableFrom(cls)) {
                return lit.getBoolean() ? "1" : "0";
            } else if (BigInteger.class.isAssignableFrom(cls)) {
                return lit.getValue().toString();
            } else if (BigDecimal.class.isAssignableFrom(cls)) {
                return lit.getValue().toString();
            } else if (Long.class.isAssignableFrom(cls) || Integer.class.isAssignableFrom(cls)) {
                return String.valueOf(lit.getLong());
            } else if (Double.class.isAssignableFrom(cls) || Float.class.isAssignableFrom(cls)) {
                return String.format("%f", lit.getDouble());
            } else {
                return "'"+lit.getLexicalForm()+"'";
            }
        }
        return null;
    }
}
