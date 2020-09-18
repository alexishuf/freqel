package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.molecules.*;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.NoMergePolicyAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.reason.tbox.OWLAPITBoxReasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;

public class APIMoleculeMatcher extends MoleculeMatcher {
    private static final Logger logger = LoggerFactory.getLogger(APIMoleculeMatcher.class);

    private final @Nonnull APIMolecule apiMolecule;
    private @Nonnull SoftReference<InputAtoms> inputAtoms = new SoftReference<>(null);

    protected static class InputAtoms {
        public @Nonnull final ImmutableSet<String> requiredAtoms, optionalAtoms;
        private @Nonnull final APIMolecule apiMolecule;

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
            String inName = apiMolecule.getElement2Input().get(atom.getName());
            boolean missing = apiMolecule.getExecutor().getInputsMissingInResult().contains(inName);
            if (requiredAtoms.contains(atom.getName())) {
                assert inName != null;
                return AtomInputAnnotation.asRequired(atom, inName).override(override)
                                          .missingInResult(missing).get();
            } else if (optionalAtoms.contains(atom.getName())) {
                assert  inName != null;
                return AtomInputAnnotation.asOptional(atom, inName).override(override)
                                          .missingInResult(missing).get();
            }
            return AtomAnnotation.of(atom);
        }

        public @Nonnull AtomAnnotation asAnnotation(@Nonnull AtomFilter filter,
                                                    @Nonnull AtomWithRole inputAtom,
                                                    @Nonnull Term input) {
            Preconditions.checkArgument(inputAtom.getRole().equals(AtomRole.INPUT));
            String filterName = filter.getName();
            String inName = apiMolecule.getElement2Input().get(filterName);
            Atom realAtom = apiMolecule.getMolecule().getAtom(inputAtom.getAtomName());
            assert realAtom != null : "Molecule has no Atom " + inputAtom.getAtomName() +
                                      " mentioned by AtomFilter " + filterName;
            if (inName == null)
                inName = apiMolecule.getElement2Input().get(inputAtom.getAtomName());
            if (inName != null) {
                boolean required = apiMolecule.getExecutor().getRequiredInputs().contains(inName);
                AtomInputAnnotation.Builder builder;
                builder = AtomInputAnnotation.builder(required, realAtom, inName).override(input);
                if (apiMolecule.getExecutor().getInputsMissingInResult().contains(inName))
                    builder.missingInResult();
                return builder.get();
            }
            return AtomAnnotation.of(realAtom);
        }

        public int size() {
            return requiredAtoms.size() + optionalAtoms.size();
        }
    }

    public APIMoleculeMatcher(@Nonnull APIMolecule apiMolecule,
                              @Nonnull TBoxReasoner reasoner) {
        super(apiMolecule.getMolecule(), reasoner, NoMergePolicyAnnotation.INSTANCE);
        this.apiMolecule = apiMolecule;
    }

    /** Creates matcher with a dummy {@link TBoxReasoner} that performs no reasoning. */
    public APIMoleculeMatcher(@Nonnull APIMolecule apiMolecule) {
        this(apiMolecule, OWLAPITBoxReasoner.structural());
    }


    public @Nonnull APIMolecule getApiMolecule() {
        return apiMolecule;
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
        private @Nonnull final InputAtoms inputAtoms;
        private @Nullable SetMultimap<String, Term> tmpAtom2Term;

        public APIState(@Nonnull CQuery query, boolean reason) {
            super(query, reason);
            inputAtoms = getInputAtoms();
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
            public void addAtomAnnotations(@Nonnull Triple triple,
                                           @Nonnull Collection<LinkMatch> matches) {
                Term s = triple.getSubject(), o = triple.getObject();
                for (LinkMatch match : matches) {
                    mQuery.annotate(s, inputAtoms.asAnnotation(match.l.s, null));
                    mQuery.annotate(o, inputAtoms.asAnnotation(match.l.o, null));
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
                        String varName = atomFilter.getVar(a);
                        assert varName != null;
                        Term onSubsumed = result.getOnSubsumed(new StdVar(varName));
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
                        mQuery.deannotateTermIf(out, a -> {
                            if (!(a instanceof AtomAnnotation)) return false;
                            if (inputName == null || !(a instanceof AtomInputAnnotation))
                                return true;
                            return ((AtomInputAnnotation)a).getInputName().equals(inputName);
                        });
                        mQuery.annotate(out, ann);
                    }
                }
            }

            @Override
            public CQuery build() {
                if (parentEG != null) {
                    mQuery.copyTermAnnotations(parentEG);
                    mQuery.copyTripleAnnotations(parentEG);
                }
                mQuery.copyTermAnnotations(parentQuery);
                if (parentEG == null) { // only check if creating the top-level exclusive group
                    getInputsFromFilters();
                    mQuery.annotate(NoMergePolicyAnnotation.INSTANCE);
                    CQuery query = mQuery;

                    // this check here is more specific than isValidEG(), as it checks inputs
                    // (not atoms) and inputs can come from FILTER()s (and that is not the
                    // case for Atoms)
                    SetMultimap<String, Term> inAssig = HashMultimap.create();
                    query.forEachTermAnnotation(AtomInputAnnotation.class, (t, a) -> {
                        Term value = a.isOverride() ? a.getOverrideValue() : t;
                        inAssig.put(a.getInputName(), value);
                    });
                    Set<String> reqInputs = apiMolecule.getExecutor().getRequiredInputs();
                    Set<String> missing = IndexedParam.getMissing(reqInputs, inAssig.keySet());
                    if (!missing.isEmpty()) {
                        logger.debug("Missing required inputs {} in query {}", missing, query);
                        return CQuery.EMPTY; // reject, since there are required inputs missing
                    }
                    if (APIMolecule.class.desiredAssertionStatus()) {
                        Set<String> s = reqInputs.stream()
                                .filter(a -> inAssig.get(a).size() > 1).collect(toSet());
                        assert s.isEmpty() : "Some required inputs are ambiguous: " + s;
                        s = apiMolecule.getExecutor().getOptionalInputs().stream()
                                .filter(a -> inAssig.get(a).size() > 1).collect(toSet());
                        assert s.isEmpty() : "Some optional inputs are ambiguous: " + s;
                    }

                    if (query.size() == parentQuery.size()) { // the whole query was matched
                        Set<String> badAssigs = reqInputs.stream()
                                .filter(k -> inAssig.get(k).stream().allMatch(Term::isVar))
                                .collect(toSet());
                        if (!badAssigs.isEmpty()) {
                            assignFromIndexedValue(inAssig, badAssigs);
                            badAssigs = badAssigs.stream()
                                    .filter(k -> inAssig.get(k).stream().allMatch(Term::isVar))
                                    .collect(toSet());
                        }
                        if (!badAssigs.isEmpty()) {
                            logger.debug("Discarding sub-query==query, since inputs {} are " +
                                         "mapped to variables. Query: {}", badAssigs, query);
                            query = CQuery.EMPTY; //no join triple left to bind the var
                        }
                    }
                    /* a deeper analysis could determine at this point whether all vars
                     * really can be assigned or not from the triples outside the EG. However,
                     * this is handled more easily at the execution phase */
                    return query;
                } else {
                    return mQuery;
                }
            }
        }

        private void assignFromIndexedValue(@Nonnull SetMultimap<String, Term> inAssig,
                                            @Nonnull Collection<String> inputs) {
            for (String key : new ArrayList<>(inAssig.keySet())) {
                IndexedParam ip = IndexedParam.parse(key);
                if (ip != null && inputs.contains(ip.base)) {
                    inAssig.removeAll(ip.base);
                    inAssig.put(ip.base, inAssig.get(key).iterator().next());
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
