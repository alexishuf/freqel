package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.MergePolicyAnnotation;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;

import javax.annotation.Nonnull;

public class RelationalMoleculeMatcher extends MoleculeMatcher {
    public RelationalMoleculeMatcher(@Nonnull Molecule molecule, @Nonnull TBoxReasoner reasoner,
                                     @Nonnull MergePolicyAnnotation mergePolicy) {
        super(molecule, reasoner, mergePolicy);
    }
    public RelationalMoleculeMatcher(@Nonnull Molecule molecule, @Nonnull TBoxReasoner reasoner) {
        this(molecule, reasoner, new AmbiguityMergePolicy());
    }

    @Override
    protected @Nonnull State createState(@Nonnull CQuery query, boolean reasoning) {
        return new BoundPredicatesState(query, reasoning);
    }

    protected class BoundPredicatesState extends State {
        public BoundPredicatesState(@Nonnull CQuery query, boolean reason) {
            super(query, reason);
        }

        @Override
        protected boolean ignoreTriple(@Nonnull Triple t) {
            return !t.getPredicate().isGround();
        }
    }
}
