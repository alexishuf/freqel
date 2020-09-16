package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Res;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class RDFTriple extends Triple {
    public RDFTriple(@Nonnull Res subject, @Nonnull URI predicate, @Nonnull Term object) {
        super(subject, predicate, object);
        checkArgument(object instanceof Lit || object instanceof Res,
                "object must be either Lit or Res");
    }

    public static @Nonnull RDFTriple fromTerms(@Nonnull Term subject, @Nonnull Term predicate,
                                               @Nonnull Term object) {
        checkArgument(subject instanceof Res, "subject must be a Res");
        checkArgument(predicate instanceof Res, "predicate must be an URI");
        checkArgument(object instanceof Res || object instanceof Lit,
                "object must be a Res or Lit");
        return new RDFTriple((Res)subject, (URI)predicate, object);
    }

    public static @Nonnull RDFTriple fromTriple(@Nonnull Triple triple) {
        return fromTerms(triple.getSubject(), triple.getPredicate(), triple.getObject());
    }
}
