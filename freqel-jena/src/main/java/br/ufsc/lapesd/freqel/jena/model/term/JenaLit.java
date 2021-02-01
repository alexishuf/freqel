package br.ufsc.lapesd.freqel.jena.model.term;

import br.ufsc.lapesd.freqel.model.RDFUtils;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.Lit;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public class JenaLit extends JenaTerm implements Lit {
    private @LazyInit JenaURI dtURI = null;
    private @LazyInit String nt;

    public JenaLit(@Nonnull RDFNode node) {
        super(node.asLiteral());
    }

    public @Nonnull Literal getLiteral() {
        return super.getModelNode().asLiteral();
    }
    @Override
    public @Nonnull RDFNode getModelNode() {
        return getLiteral();
    }

    @Override
    public @Nonnull String getLexicalForm() {
        return getModelNode().asLiteral().getLexicalForm();
    }

    @Override
    public @Nonnull JenaURI getDatatype() {
        JenaURI local = dtURI;
        if (local == null) {
            String uri = getModelNode().asLiteral().getDatatypeURI();
            if (uri == null)
                uri = XSDDatatype.XSDstring.getURI();
            local = dtURI = JenaURICache.getInstance().getURI(uri);
        }
        return local;
    }

    @Nullable
    @Override
    public String getLangTag() {
        String lang = getModelNode().asLiteral().getLanguage();
        return lang.equals("") ? null : lang;
    }

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public String toString() {
        return toTurtle(StdPrefixDict.DEFAULT);
    }

    @Override
    public @Nonnull String toNT() {
        String local = this.nt;
        if (local == null)
            this.nt = local = RDFUtils.toNT(this);
        return local;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Lit) && toNT().equals(((Lit)obj).toNT());
    }

    @Override
    public int hashCode() {
        return toNT().hashCode();
    }
}
