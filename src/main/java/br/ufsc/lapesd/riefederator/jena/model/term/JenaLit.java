package br.ufsc.lapesd.riefederator.jena.model.term;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.WeakReference;

@Immutable
public class JenaLit extends JenaTerm implements Lit {
    private @LazyInit JenaURI dtURI = null;
    @SuppressWarnings("Immutable")
    private WeakReference<String> nt = new WeakReference<>(null);

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
    public String toString() {
        return toTurtle(StdPrefixDict.DEFAULT);
    }

    @Override
    public @Nonnull String toNT() {
        String strong = nt.get();
        if (strong == null)
            nt = new WeakReference<>(strong = RDFUtils.toNT(this));
        return strong;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Lit) && toNT().equals(((Lit)obj).toNT());
    }
}
