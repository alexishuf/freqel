package br.ufsc.lapesd.riefederator.model.jena;

import br.ufsc.lapesd.riefederator.model.jena.prefix.PrefixMappingDict;
import br.ufsc.lapesd.riefederator.model.jena.term.*;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Res;
import br.ufsc.lapesd.riefederator.model.term.URI;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.jena.datatypes.TypeMapper.getInstance;
import static org.apache.jena.rdf.model.ResourceFactory.*;

@SuppressWarnings("WeakerAccess")
public class JenaWrappers {
    private static Logger logger = LoggerFactory.getLogger(JenaTerm.class);

    /* ~~~~~~~~~ from(RDFNode) ~~~~~~~~~ */

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static JenaTerm from(RDFNode node) {
        if (node == null)           return null;
        else if (node.isLiteral())  return from(node.asLiteral());
        else if (node.isResource()) return from(node.asResource());
        throw new UnsupportedOperationException("Do not know how to handle "+node);
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static JenaURI fromURIResource(Resource resource) {
        return resource == null ? null : new JenaURI(resource);
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static JenaBlank fromAnon(Resource resource) {
        return resource == null ? null : new JenaBlank(resource);
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static JenaRes from(Resource resource) {
        if (resource == null) return null;
        return  resource.isAnon() ? fromAnon(resource) : fromURIResource(resource);
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static JenaLit from(Literal literal) {
        return literal == null ? null : new JenaLit(literal);
    }

    /* ~~~~~~~~~ to(JenaTerm) ~~~~~~~~~ */

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static RDFNode to(JenaTerm   t) {return t == null ? null : t.getNode(); }
    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Resource to(JenaRes   t) {return t == null ? null : t.getNode().asResource();}
    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Resource to(JenaBlank t) {return t == null ? null : t.getNode().asResource();}
    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Resource to(JenaURI   t) {return t == null ? null : t.getNode().asResource();}
    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Literal  to(JenaLit   t) {return t == null ? null : t.getNode().asLiteral(); }

    /* ~~~~~~~~~ to(Term) ~~~~~~~~~ */

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Resource to(Res res) {
        if (res == null) return null;
        if (res instanceof JenaRes) return to((JenaRes)res);
        return res.isAnon() ? to(res.asBlank()) : to(res.asURI());
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Resource to(Blank blank) {
        if (blank == null) return null;
        if (blank instanceof JenaBlank) return to((JenaRes) blank);
        return createResource();
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Resource to(URI uri) {
        if (uri == null) return null;
        if (uri instanceof JenaURI) return to((JenaRes) uri);
        return createResource(uri.getURI());
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static Literal to(Lit l) {
        if      (l == null)            return null;
        else if (l instanceof JenaLit) return to((JenaLit)l);
        else if (l.getLangTag() != null)
            return createLangLiteral(l.getLexicalForm(), l.getLangTag());

        String dtURI = l.getDatatype().getURI();
        RDFDatatype dt = getInstance().getTypeByName(dtURI);
        if (dt == null) {
            logger.warn("Unknown datatype {} for {}. Making it a plain literal", dtURI, l);
            return createPlainLiteral(l.getLexicalForm());
        }
        return createTypedLiteral(l.getLexicalForm(), dt);
    }

    /* ~~~~~~~~~ from(PrefixMapping) ~~~~~~~~~ */

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static PrefixMappingDict from(PrefixMapping mapping) {
        return mapping == null ? null : new PrefixMappingDict(mapping);
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static PrefixMapping to(PrefixDict dict) {
        if (dict == null) return null;
        PrefixMappingImpl m = new PrefixMappingImpl();
        for (Map.Entry<String, String> entry : dict.entries())
            m.setNsPrefix(entry.getKey(), entry.getValue());
        return m;
    }
}
