package br.ufsc.lapesd.riefederator.model.term;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.lang.ref.WeakReference;

@Immutable
public abstract class AbstractLit implements Lit {
    @SuppressWarnings("Immutable")
    private @Nonnull WeakReference<String> nt = new WeakReference<>(null);

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public @Nonnull String toNT() {
        String strong = nt.get();
        if (strong == null)
            nt = new WeakReference<>(strong = RDFUtils.toNT(this));
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
