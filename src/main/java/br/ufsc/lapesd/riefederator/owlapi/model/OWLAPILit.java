package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;

@Immutable
public class OWLAPILit extends OWLAPITerm implements Lit {
    @SuppressWarnings("Immutable")
    private @Nonnull SoftReference<String> nt = new SoftReference<>(null);


    public OWLAPILit(@Nonnull OWLObject object) {
        super(object);
        Preconditions.checkArgument(object instanceof OWLLiteral, "object must be a OWLLiteral");
    }

    @Override
    public @Nonnull String getLexicalForm() {
        return asOWLLiteral().getLiteral();
    }

    @Override
    public @Nonnull URI getDatatype() {
        return new OWLAPIIRI(asOWLLiteral().getDatatype().getIRI());
    }

    @Override
    public @Nullable String getLangTag() {
        OWLLiteral lit = asOWLLiteral();
        return lit.hasLang() ? lit.getLang() : null;
    }

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public @Nonnull String toNT() {
        String strong = nt.get();
        if (strong == null)
            nt = new SoftReference<>(strong = RDFUtils.toNT(this));
        return strong;
    }

    @Override
    public int hashCode() {
        return toNT().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Lit) && toNT().equals(((Lit)obj).toNT());
    }
}
