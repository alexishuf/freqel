package br.ufsc.lapesd.riefederator.model.term;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public interface URI extends Res {
    /**
     * Gets the full expanded URI of a resouce
     */
    @Nonnull String getURI();

    default @Nonnull String toNT() {
        return RDFUtils.toNT(this);
    }
    default @Nonnull String toTurtle(@Nonnull PrefixDict dict) {
        return RDFUtils.toTurtle(this, dict);
    }
}
