package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.description.molecules.tags.ElementTag;
import br.ufsc.lapesd.freqel.model.term.Term;

import javax.annotation.Nonnull;

public class AmbiguousTagException extends RuntimeException {

    public AmbiguousTagException(@Nonnull Class<? extends ElementTag> tagClass,
                                 @Nonnull Term term) {
        super("Ambiguous "+tagClass.getSimpleName()+" for term "+term);
    }

}
