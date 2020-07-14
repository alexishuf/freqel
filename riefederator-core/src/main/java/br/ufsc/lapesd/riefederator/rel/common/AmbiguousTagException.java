package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.description.molecules.ElementTag;
import br.ufsc.lapesd.riefederator.model.term.Term;

import javax.annotation.Nonnull;

public class AmbiguousTagException extends RuntimeException {
    private final @Nonnull Class<? extends ElementTag> tagClass;
    private final @Nonnull Term term;

    public AmbiguousTagException(@Nonnull Class<? extends ElementTag> tagClass,
                                 @Nonnull Term term) {
        super("Ambiguous "+tagClass.getSimpleName()+" for term "+term);
        this.tagClass = tagClass;
        this.term = term;
    }

}
