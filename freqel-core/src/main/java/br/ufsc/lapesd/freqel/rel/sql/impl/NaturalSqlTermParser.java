package br.ufsc.lapesd.freqel.rel.sql.impl;

import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.rel.common.RelationalTermParser;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.impl.LiteralImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.sys.JenaSystem;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;

/**
 * Implements natural mapping [1] of SQL objects to RDF literals.
 *
 * [1]: https://www.w3.org/TR/2012/REC-r2rml-20120927/#natural-mapping
 */
public class NaturalSqlTermParser implements RelationalTermParser {
    public static final @Nonnull NaturalSqlTermParser INSTANCE = new NaturalSqlTermParser();
    private static final Map<Class<?>, Function<Object, RDFNode>> cls2converter;

    private static @Nonnull RDFNode convertDouble(@Nonnull Object o) {
        double value;
        if      (o instanceof Float ) value = (double)(float)o;
        else if (o instanceof Double) value = (double)o;
        else throw new IllegalArgumentException("Expected Double or Float, got "+o);
        String lex = String.format("%E", value);
        lex = lex.replace("E+", "E")
                 .replaceAll("E0+([0-9])$", "E$1")
                 .replaceAll("\\.00+E", ".0E")
                 .replaceAll("\\.(\\d*[1-9])0+E", ".$1E");
        return createTypedLiteral(lex, XSDDatatype.XSDdouble);
    }

    private static @Nonnull RDFNode convertDateTime(@Nonnull Object o) {
        Timestamp time = (Timestamp) o;
        String lex = time.toString().replace(" ", "T").replaceAll("\\.0+$", "");
        return createTypedLiteral(lex, XSDDatatype.XSDdateTime);
    }

    private static @Nonnull RDFNode convertBinary(@Nonnull Object o) {
        byte[] bytes = (byte[]) o;
        StringBuilder builder = new StringBuilder(bytes.length*2);
        for (byte b : bytes)
            builder.append(String.format("%02X", b));
        return createTypedLiteral(builder.toString(), XSDDatatype.XSDhexBinary);
    }

    static {
        JenaSystem.init();
        Map<Class<?>, Function<Object, RDFNode>> map = new HashMap<>();
        map.put(Byte.class,    o -> createTypedLiteral(o.toString(), XSDDatatype.XSDinteger));
        map.put(Short.class,   o -> createTypedLiteral(o.toString(), XSDDatatype.XSDinteger));
        map.put(Integer.class, o -> createTypedLiteral(o.toString(), XSDDatatype.XSDinteger));
        map.put(Long.class,    o -> createTypedLiteral(o.toString(), XSDDatatype.XSDinteger));
        map.put(Float.class,   NaturalSqlTermParser::convertDouble);
        map.put(Double.class,  NaturalSqlTermParser::convertDouble);
        map.put(Boolean.class, o -> createTypedLiteral(o.toString(), XSDDatatype.XSDboolean));
        map.put(Date.class,    o -> createTypedLiteral(o.toString(), XSDDatatype.XSDdate));
        map.put(Time.class,    o -> createTypedLiteral(o.toString(), XSDDatatype.XSDtime));
        map.put(Timestamp.class, NaturalSqlTermParser::convertDateTime);
        map.put(byte[].class,  NaturalSqlTermParser::convertBinary);
        cls2converter = map;
    }

    @Override
    public @Nullable Term parseTerm(@Nullable Object sqlObject) {
        if (sqlObject == null || (sqlObject instanceof Term))
            return (Term)sqlObject;
        return JenaWrappers.fromJena(parseNode(sqlObject));
    }

    @Override
    public @Nullable RDFNode parseNode(@Nullable Object sqlObject) {
        if (sqlObject == null || sqlObject instanceof RDFNode) {
            return (RDFNode)sqlObject;
        } else if (sqlObject instanceof Node) {
            Node node = (Node) sqlObject;
            if (node.isURI() || node.isBlank())
                return new ResourceImpl(node, null);
            else if (node.isLiteral())
                return new LiteralImpl(node, null);
            else
                throw new IllegalArgumentException("Cannot convert "+node+" to an RDFNode");
        } else if (sqlObject instanceof Term) {
            return JenaWrappers.toJena((Term)sqlObject);
        }
        Function<Object, RDFNode> converter = cls2converter.get(sqlObject.getClass());
        if (converter != null)
            return converter.apply(sqlObject);
        return createTypedLiteral(sqlObject); //fallback to jena
    }
}
