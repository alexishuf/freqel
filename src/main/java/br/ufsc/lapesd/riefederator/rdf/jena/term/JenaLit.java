package br.ufsc.lapesd.riefederator.rdf.jena.term;

import br.ufsc.lapesd.riefederator.rdf.RDFUtils;
import br.ufsc.lapesd.riefederator.rdf.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.rdf.term.Lit;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static br.ufsc.lapesd.riefederator.rdf.RDFUtils.escapeLexicalForm;

@Immutable
public class JenaLit extends JenaTerm implements Lit {
    private @LazyInit JenaURI dtURI = null;

    public JenaLit(@Nonnull RDFNode node) {
        super(node.asLiteral());
    }

    public @Nonnull Literal getLiteral() {
        return super.getNode().asLiteral();
    }
    @Override
    public @Nonnull RDFNode getNode() {
        return getLiteral();
    }

    @Override
    public @Nonnull String getLexicalForm() {
        return getNode().asLiteral().getLexicalForm();
    }

    @Override
    public @Nonnull JenaURI getDatatype() {
        JenaURI local = dtURI;
        if (local == null) {
            String uri = getNode().asLiteral().getDatatypeURI();
            if (uri == null)
                uri = XSDDatatype.XSDstring.getURI();
            local = dtURI = JenaURICache.getInstance().getURI(uri);
        }
        return local;
    }

    @Nullable
    @Override
    public String getLangTag() {
        String lang = getNode().asLiteral().getLanguage();
        return lang.equals("") ? null : lang;
    }

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        StringBuilder builder = new StringBuilder();
        builder.append('"').append(escapeLexicalForm(getLexicalForm())).append('"');

        String lang = getLiteral().getLanguage();
        if (lang != null && !lang.isEmpty())
            return builder.append("@").append(lang).toString();

        String uri = getLiteral().getDatatypeURI();
        builder.append("^^").append(dict.shorten(uri).toString("<" + uri + ">"));
        return builder.toString();
    }
}
