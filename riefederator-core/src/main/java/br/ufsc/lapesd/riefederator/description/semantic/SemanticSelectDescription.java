package br.ufsc.lapesd.riefederator.description.semantic;

import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.MissingCapabilityException;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

public class SemanticSelectDescription extends SelectDescription implements SemanticDescription {
    private final @Nonnull TBoxReasoner reasoner;

    public SemanticSelectDescription(@Nonnull CQEndpoint endpoint,
                                     @Nonnull TBoxReasoner tBoxReasoner)
            throws MissingCapabilityException  {
        this(endpoint, false, tBoxReasoner);
    }

    public SemanticSelectDescription(@Nonnull CQEndpoint endpoint, boolean fetchClasses,
                                     @Nonnull TBoxReasoner tBoxReasoner)
            throws MissingCapabilityException  {
        super(endpoint, fetchClasses);
        this.reasoner = tBoxReasoner;
    }

    @Override
    public @Nonnull SemanticCQueryMatch semanticMatch(@Nonnull CQuery query) {
        SemanticCQueryMatch.Builder b = SemanticCQueryMatch.builder(query);
        if (!initSync())
            return b.build();
        Set<Term> predicates = Objects.requireNonNull(this.predicates);
        for (Triple triple : query) {
            Term p = triple.getPredicate(), o = triple.getObject();
            if (classes != null && p.equals(TYPE) && o.isURI()) {
                boolean[] pending = {false};
                boolean added = classes.contains(o);
                if (added) {
                    b.addTriple(triple);
                    b.addAlternative(triple, triple);
                }
                reasoner.subClasses(o).filter(classes::contains).map(triple::withObject)
                        .forEach(a -> {pending[0] = true; b.addAlternative(triple, a);});
                if (pending[0] && !added)
                    b.addTriple(triple);
            } else if (p.isVar()) {
                b.addTriple(triple);
            } else {
                boolean[] pending = {false};
                boolean added = predicates.contains(p);
                if (added) {
                    b.addTriple(triple);
                    b.addAlternative(triple, triple);
                }
                reasoner.subProperties(p).filter(predicates::contains).map(triple::withPredicate)
                        .forEach(a -> {pending[0] = true; b.addAlternative(triple, a);});
                if (pending[0] && !added)
                    b.addTriple(triple);
            }
        }
        return b.build();
    }
}
