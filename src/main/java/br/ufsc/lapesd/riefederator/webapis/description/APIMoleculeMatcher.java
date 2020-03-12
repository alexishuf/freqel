package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.molecules.*;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.reason.tbox.OWLAPITBoxReasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;

public class APIMoleculeMatcher extends MoleculeMatcher {
    private final @Nonnull APIMolecule apiMolecule;
    private @Nonnull SoftReference<InputAtoms> inputAtoms = new SoftReference<>(null);

    protected static class InputAtoms {
        public @Nonnull ImmutableSet<String> requiredAtoms, optionalAtoms;
        private @Nonnull APIMolecule apiMolecule;

        public InputAtoms(@Nonnull APIMolecule apiMolecule) {
            Predicate<String> isAtom = apiMolecule.getMolecule().getAtomMap()::containsKey;
            ImmutableSet.Builder<String> requiredBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<String> optionalBuilder = ImmutableSet.builder();
            Set<String> requiredInputs = apiMolecule.getExecutor().getRequiredInputs();
            Set<String> optionalInputs = apiMolecule.getExecutor().getOptionalInputs();
            for (Map.Entry<String, String> e : apiMolecule.getElement2Input().entrySet()) {
                String name = e.getKey();
                if (!isAtom.test(name))
                    continue;
                String input = e.getValue();
                if (requiredInputs.contains(input))
                    requiredBuilder.add(name);
                else if (optionalInputs.contains(input))
                    optionalBuilder.add(name);
            }
            requiredAtoms = requiredBuilder.build();
            optionalAtoms = optionalBuilder.build();
            this.apiMolecule = apiMolecule;
        }

        public @Nonnull AtomAnnotation asAnnotation(@Nonnull Atom atom, @Nullable Term override) {
            String inputName = apiMolecule.getElement2Input().get(atom.getName());
            if (requiredAtoms.contains(atom.getName())) {
                assert inputName != null;
                return AtomAnnotation.asRequired(atom, inputName, override);
            } else if (optionalAtoms.contains(atom.getName())) {
                assert  inputName != null;
                return AtomAnnotation.asOptional(atom, inputName, override);
            }
            return AtomAnnotation.of(atom);
        }

        public @Nonnull AtomAnnotation asAnnotation(@Nonnull AtomFilter filter,
                                                    @Nonnull AtomWithRole inputAtom,
                                                    @Nonnull Term input) {
            Preconditions.checkArgument(inputAtom.getRole().equals(AtomRole.INPUT));
            String filterName = filter.getName();
            String inputName = apiMolecule.getElement2Input().get(filterName);
            Atom realAtom = apiMolecule.getMolecule().getAtom(inputAtom.getAtomName());
            assert realAtom != null : "Molecule has no Atom " + inputAtom.getAtomName() +
                                      " mentioned by AtomFilter " + filterName;
            if (inputName == null)
                inputName = apiMolecule.getElement2Input().get(inputAtom.getAtomName());
            if (inputName != null) {
                Set<String> requiredInputs = apiMolecule.getExecutor().getRequiredInputs();
                if (requiredInputs.contains(inputName))
                    return AtomAnnotation.asRequired(realAtom, inputName, input);
                else
                    return AtomAnnotation.asOptional(realAtom, inputName, input);
            }
            return AtomAnnotation.of(realAtom);
        }

        public int size() {
            return requiredAtoms.size() + optionalAtoms.size();
        }
    }

    public APIMoleculeMatcher(@Nonnull APIMolecule apiMolecule,
                              @Nonnull TBoxReasoner reasoner) {
        super(apiMolecule.getMolecule(), reasoner);
        this.apiMolecule = apiMolecule;
    }

    /** Creates matcher with a dummy {@link TBoxReasoner} that performs no reasoning. */
    public APIMoleculeMatcher(@Nonnull APIMolecule apiMolecule) {
        this(apiMolecule, OWLAPITBoxReasoner.structural());
    }

    protected @Nonnull InputAtoms getInputAtoms() {
        InputAtoms strong = this.inputAtoms.get();
        if (strong == null)
            this.inputAtoms = new SoftReference<>(strong = new InputAtoms(apiMolecule));
        return strong;
    }

    @Override
    public @Nonnull SemanticCQueryMatch semanticMatch(@Nonnull CQuery query) {
        return new APIState(query, true).matchExclusive().build();
    }

    @Override
    public @Nonnull CQueryMatch match(@Nonnull CQuery query) {
        return new APIState(query, false).matchExclusive().build();
    }

    @Override
    public @Nonnull String toString() {
        return String.format("APIMoleculeMatcher(reasoner=%s, apiMolecule=%s)",
                getReasoner(), getMolecule());
    }

    protected class APIState extends State {
        private @Nonnull InputAtoms inputAtoms;
        private @Nullable SetMultimap<String, Term> tmpAtom2Term;

        public APIState(@Nonnull CQuery query, boolean reason) {
            super(query, reason);
            inputAtoms = getInputAtoms();
            reuseParentForEG = false;
        }

        @Override
        protected boolean isValidEG(CQuery query, List<List<LinkMatch>> matchLists) {
            fillAtom2Term(matchLists);
            assert tmpAtom2Term != null;
            return inputAtoms.requiredAtoms.stream()
                    .allMatch(atomName -> tmpAtom2Term.get(atomName).size() == 1);
        }

        private void fillAtom2Term(List<List<LinkMatch>> matchLists) {
            if (tmpAtom2Term == null) tmpAtom2Term = HashMultimap.create();
            else tmpAtom2Term.clear();
            matchLists.stream().flatMap(List::stream).forEach(m -> {
                tmpAtom2Term.put(m.l.s.getName(), m.triple.getSubject());
                tmpAtom2Term.put(m.l.o.getName(), m.triple.getObject());
            });
        }

        @Override
        protected boolean isAmbiguousEG(CQuery query, List<List<LinkMatch>> matchLists) {
            fillAtom2Term(matchLists);
            assert tmpAtom2Term != null;
            return inputAtoms.requiredAtoms.stream()
                    .anyMatch(atomName -> tmpAtom2Term.get(atomName).size() > 1);
        }

        protected class APIEGQueryBuilder extends EGQueryBuilder {
            private final @Nullable CQuery parentEG;

            public APIEGQueryBuilder(int sizeHint) {
                super(sizeHint);
                parentEG = null;
            }
            public APIEGQueryBuilder(@Nonnull CQuery parentEG,
                                     @Nonnull EGQueryBuilder parentBuilder) {
                super(parentEG.size());
                Preconditions.checkArgument(parentBuilder instanceof APIEGQueryBuilder);
                APIEGQueryBuilder apiBuilder = (APIEGQueryBuilder) parentBuilder;
                this.parentEG = parentEG;
                this.term2atom.putAll(apiBuilder.term2atom);
                this.subsumption2matched.putAll(apiBuilder.subsumption2matched);
            }

            @Override
            public void add(@Nonnull Triple triple, @Nonnull Collection<LinkMatch> matches) {
                super.add(triple, matches);
                Term s = triple.getSubject(), o = triple.getObject();
                for (LinkMatch match : matches) {
                    builder.annotate(s, inputAtoms.asAnnotation(match.l.s, null));
                    builder.annotate(o, inputAtoms.asAnnotation(match.l.o, null));
                }
            }

            /**
             * Scans through matched {@link AtomFilter}s to override {@link AtomAnnotation}s.
             */
            private void getInputsFromFilters() {
                assert subsumption2matched.keySet().stream()
                        .allMatch(SPARQLFilter.SubsumptionResult::getValue);
                for (Map.Entry<SPARQLFilter.SubsumptionResult, AtomFilter> e
                        : subsumption2matched.entrySet()) {
                    SPARQLFilter.SubsumptionResult result = e.getKey();
                    AtomFilter atomFilter = e.getValue();
                    Map<AtomWithRole, Term> atom2queryTerm = new HashMap<>();
                    atomFilter.getAtoms().forEach(a -> {
                        String var = atomFilter.getVar(a);
                        assert var != null;
                        Term term = atomFilter.getSPARQLFilter().get(var);
                        assert term != null;
                        Term onSubsumed = result.getOnSubsumed(term);
                        atom2queryTerm.put(a, onSubsumed);
                    });
                    for (Map.Entry<AtomWithRole, Term> e2 : atom2queryTerm.entrySet()) {
                        AtomWithRole atom = e2.getKey();
                        Term input = e2.getValue();
                        if (!atom.getRole().equals(AtomRole.INPUT)) continue;
                        if (input.isVar()) continue;
                        Term out = atom2queryTerm.get(AtomRole.OUTPUT.wrap(atom.getAtomName()));
                        if (out == null) continue;
                        AtomAnnotation ann = inputAtoms.asAnnotation(atomFilter, atom, input);
                        String inputName = ann instanceof AtomInputAnnotation ?
                                ((AtomInputAnnotation) ann).getInputName() : null;
                        builder.reannotate(out, a -> {
                            if (!(a instanceof AtomAnnotation)) return false;
                            if (inputName == null || !(a instanceof AtomInputAnnotation))
                                return true;
                            return ((AtomInputAnnotation)a).getInputName().equals(inputName);
                        }, ann);
                    }
                }
            }

            @Override
            public CQuery build() {
                if (parentEG != null) {
                    parentEG.forEachTermAnnotation(builder::annotate);
                    parentEG.forEachTripleAnnotation(builder::annotate);
                }
                prepareBuild();
                if (parentEG == null) { // only check if creating the top-level exclusive group
                    getInputsFromFilters();
                    CQuery query = builder.build();

                    if (APIMolecule.class.desiredAssertionStatus()) {
                        SetMultimap<String, Term> inAssignments = HashMultimap.create();
                        query.forEachTermAnnotation(AtomInputAnnotation.class, (t, a) -> {
                            Term value = a.isOverride() ? a.getOverrideValue() : t;
                            inAssignments.put(a.getInputName(), value);
                        });
                        Set<String> reqInputs = apiMolecule.getExecutor().getRequiredInputs();
                        assert inAssignments.keySet().containsAll(reqInputs) :
                                "Upstream did not annotate some input atoms with AtomAnnotation: "
                                        + TreeUtils.setMinus(reqInputs, inAssignments.keySet());
                        Set<String> s = reqInputs.stream()
                                .filter(a -> inAssignments.get(a).size() > 1).collect(toSet());
                        assert s.isEmpty() : "Some required inputs are ambiguous: " + s;
                        s = apiMolecule.getExecutor().getOptionalInputs().stream()
                                .filter(a -> inAssignments.get(a).size() > 1).collect(toSet());
                        assert s.isEmpty() : "Some optional inputs are ambiguous: " + s;
                    }

                    boolean[] hasVar = {false};
                    query.forEachTermAnnotation(AtomInputAnnotation.class, (t, a) -> {
                        Term value = a.isOverride() ? a.getOverrideValue() : t;
                        assert value != null;
                        hasVar[0] |= a.isRequired() && value.isVar();
                    });
                    if (query.size() == parentQuery.size() && hasVar[0])
                        query = CQuery.EMPTY; //no join triple left to bind the var
                    /* a deeper analysis could determine at this point whether all vars
                     * really can be assigned or not from the triples outside the EG. However,
                     * this is handled more easily at the execution phase */
                    return query;
                } else {
                    return builder.build();
                }
            }
        }

        @Override
        protected @Nonnull EGQueryBuilder createEGQueryBuilder(int sizeHint) {
            return new APIEGQueryBuilder(sizeHint);
        }

        @Override
        protected @Nonnull EGQueryBuilder
        createEGQueryBuilder(@Nonnull CQuery parent, @Nonnull EGQueryBuilder parentBuilder) {
            return new APIEGQueryBuilder(parent, parentBuilder);
        }

    }
}
