package br.ufsc.lapesd.riefederator.jena.model.term.node;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Literal;
import org.apache.jena.graph.Node_URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public class JenaLitNode extends JenaNodeTerm implements Lit {
    private @LazyInit @Nullable JenaURINode dt;
    private @LazyInit @Nullable String nt;

    public JenaLitNode(@Nonnull Node_Literal node) {
        super(node);
    }

    @Override
    public @Nonnull String getLexicalForm() {
        return node.getLiteralLexicalForm();
    }

    @Override
    public @Nonnull JenaURINode getDatatype() {
        JenaURINode local = this.dt;
        if (local == null) {
            String uri = node.getLiteralDatatypeURI();
            this.dt = local = new JenaURINode((Node_URI) NodeFactory.createURI(uri));
        }
        return local;
    }

    @Override
    public @Nullable String getLangTag() {
        String lang = node.getLiteralLanguage();
        return lang.isEmpty() ? null : lang;
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
