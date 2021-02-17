package br.ufsc.lapesd.freqel.reason.tbox.replacements.generators;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementContext;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class SubTermReplacementGenerator extends PruningReplacementGenerator {
    private @Nullable TBox tBox;

    @Inject public SubTermReplacementGenerator(@Nonnull ReplacementPruner pruner) {
        super(pruner);
    }
    public SubTermReplacementGenerator() { }

    public @Nullable TBox setTBox(@Nonnull TBox tBox) {
        TBox old = this.tBox;
        this.tBox = tBox;
        return old;
    }

    private boolean isPredicate(@Nonnull Term term, @Nonnull Collection<Triple> triples) {
        for (Triple triple : triples) {
            if (triple.getPredicate().equals(term))
                return true;
        }
        return false;
    }

    private boolean isClass(@Nonnull Term term, @Nonnull Collection<Triple> triples) {
        for (Triple triple : triples) {
            if (triple.getPredicate().equals(V.RDF.type) && triple.getObject().equals(term))
                return true;
        }
        return false;
    }

    @Override
    protected @Nonnull Iterator<Term> getAlternatives(@Nonnull Term original,
                                                      @Nonnull ReplacementContext ctx) {
        if (tBox == null) {
            return Collections.emptyIterator();
        } else if (isPredicate(original, ctx.getTriples())) {
            return tBox.withSubProperties(original).iterator();
        } else if (isClass(original, ctx.getTriples())) {
            return tBox.withSubClasses(original).iterator();
        }
        return Collections.emptyIterator();
    }
}
