package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.annotations.MergePolicyAnnotation;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;

import javax.annotation.Nonnull;

public class RelationalMoleculeMatcher extends MoleculeMatcher {
    public RelationalMoleculeMatcher(@Nonnull Molecule molecule, @Nonnull TBox reasoner,
                                     @Nonnull MergePolicyAnnotation mergePolicy) {
        super(molecule, reasoner, mergePolicy);
    }
    public RelationalMoleculeMatcher(@Nonnull Molecule molecule, @Nonnull TBox reasoner) {
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
