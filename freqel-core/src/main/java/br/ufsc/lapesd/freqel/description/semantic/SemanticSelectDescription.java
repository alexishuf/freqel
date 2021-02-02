package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.MissingCapabilityException;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

import static br.ufsc.lapesd.freqel.description.semantic.SemanticCQueryMatch.EMPTY;
import static br.ufsc.lapesd.freqel.description.semantic.SemanticCQueryMatch.builder;

public class SemanticSelectDescription extends SelectDescription implements SemanticDescription {
    private final @Nonnull TBox reasoner;

    public SemanticSelectDescription(@Nonnull CQEndpoint endpoint,
                                     @Nonnull TBox tBox)
            throws MissingCapabilityException  {
        this(endpoint, false, tBox);
    }

    public SemanticSelectDescription(@Nonnull CQEndpoint endpoint, boolean fetchClasses,
                                     @Nonnull TBox tBox)
            throws MissingCapabilityException  {
        super(endpoint, fetchClasses);
        this.reasoner = tBox;
    }

    @Override
    public @Nonnull SemanticCQueryMatch semanticMatch(@Nonnull CQuery query) {
        SemanticCQueryMatch.Builder[] b = {null};
        if (!initSync())
            return SemanticCQueryMatch.EMPTY;
        Set<Term> predicates = Objects.requireNonNull(this.predicates);
        for (Triple triple : query) {
            Term p = triple.getPredicate(), o = triple.getObject();
            if (classes != null && p.equals(TYPE) && o.isURI()) {
                boolean[] pending = {false};
                boolean added = classes.contains(o);
                if (added) {
                    (b[0] == null ? b[0] = builder(query) : b[0]).addTriple(triple);
                    b[0].addAlternative(triple, triple);
                }
                reasoner.subClasses(o).filter(classes::contains).map(triple::withObject)
                        .forEach(a -> {
                            pending[0] = true;
                            (b[0] == null ? b[0] = builder(query) : b[0]).addAlternative(triple, a);
                        });
                if (pending[0] && !added)
                    (b[0] == null ? b[0] = builder(query) : b[0]).addTriple(triple);
            } else if (p.isVar()) {
                (b[0] == null ? b[0] = builder(query) : b[0]).addTriple(triple);
            } else {
                boolean[] pending = {false};
                boolean added = predicates.contains(p);
                if (added) {
                    (b[0] == null ? b[0] = builder(query) : b[0]).addTriple(triple);
                    b[0].addAlternative(triple, triple);
                }
                reasoner.subProperties(p).filter(predicates::contains).map(triple::withPredicate)
                        .forEach(a -> {
                            pending[0] = true;
                            (b[0] == null ? b[0] = builder(query) : b[0]).addAlternative(triple, a);
                        });
                if (pending[0] && !added)
                    (b[0] == null ? b[0] = builder(query) : b[0]).addTriple(triple);
            }
        }
        return b[0] == null ? EMPTY : b[0].build();
    }
}
