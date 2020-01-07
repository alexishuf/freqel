package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.TripleAnnotation;
import br.ufsc.lapesd.riefederator.reason.tbox.OWLAPITBoxReasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;
import com.google.common.collect.*;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;

public class APIMoleculeMatcher extends MoleculeMatcher {
    private final @Nonnull APIMolecule apiMolecule;
    private @Nonnull SoftReference<InputAtoms> inputAtoms = new SoftReference<>(null);

    protected static class InputAtoms {
        public @Nonnull ImmutableSet<String> required, optional;

        public InputAtoms(@Nonnull APIMolecule apiMolecule) {
            ImmutableSet.Builder<String> requiredBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<String> optionalBuilder = ImmutableSet.builder();
            Set<String> requiredInputs = apiMolecule.getExecutor().getRequiredInputs();
            Set<String> optionalInputs = apiMolecule.getExecutor().getOptionalInputs();
            for (Map.Entry<String, String> e : apiMolecule.getAtom2input().entrySet()) {
                String atomName = e.getKey();
                String input = e.getValue();
                if (requiredInputs.contains(input))
                    requiredBuilder.add(atomName);
                else if (optionalInputs.contains(input))
                    optionalBuilder.add(atomName);
            }
            required = requiredBuilder.build();
            optional = optionalBuilder.build();
        }

        public @Nonnull AtomAnnotation asAnnotation(@Nonnull Atom atom) {
            if (required.contains(atom.getName()))
                return AtomAnnotation.asRequired(atom);
            else if (optional.contains(atom.getName()))
                return AtomAnnotation.asOptional(atom);
            return new AtomAnnotation(atom);
        }

        public int size() {
            return required.size() + optional.size();
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
                getReasoner(), getMolecule() );
    }

    protected class APIState extends State {
        private @Nonnull InputAtoms inputAtoms;

        public APIState(@Nonnull CQuery query, boolean reason) {
            super(query, reason);
            inputAtoms = getInputAtoms();
        }

        protected class APIEGQueryBuilder extends EGQueryBuilder {
            private final @Nullable CQuery parentEG;

            public APIEGQueryBuilder(int sizeHint) {
                super(sizeHint);
                parentEG = null;
                reuseParentForEG = false;
            }
            public APIEGQueryBuilder(@Nonnull CQuery parentEG) {
                super(parentEG.size());
                this.parentEG = parentEG;
                reuseParentForEG = false;
            }

            @Override
            public void add(@Nonnull Triple triple, @Nonnull Collection<LinkMatch> matches) {
                super.add(triple, matches);
                Term s = triple.getSubject(), o = triple.getObject();
                for (LinkMatch match : matches) {
                    builder.annotate(s, inputAtoms.asAnnotation(match.l.s));
                    builder.annotate(o, inputAtoms.asAnnotation(match.l.o));
                }
            }

            @Override
            public CQuery build() {
                if (parentEG != null) {
                    parentEG.forEachTermAnnotation(builder::annotate);
                    parentEG.forEachTripleAnnotation(builder::annotate);
                }
                CQuery query = super.build();
                if (parentEG == null) { // only check if creating the top-level exclusive group
                    boolean[] hasVar = {false};
                    Multiset<String> observed = HashMultiset.create(inputAtoms.size());
                    query.forEachTermAnnotation(AtomInputAnnotation.class, (t, a) -> {
                        if (a.isRequired()) {
                            observed.add(a.getAtomName());
                            if (t.isVar()) hasVar[0] = true;
                        }
                    });
                    if (!observed.containsAll(inputAtoms.required))
                        query = CQuery.EMPTY; //unsatisfiable
                    if (inputAtoms.required.stream().anyMatch(a -> observed.count(a) > 1))
                        query = CQuery.EMPTY; //ambiguous
                    if (query.size() == parentQuery.size() && hasVar[0])
                        query = CQuery.EMPTY; //no join triple left to bind the var
                    /* a deeper analysis could determine at this point whether all vars
                     * really can be assigned or not from the triples outside the EG. However,
                     * this is handled easier at execution phase */
                }
                return query;
            }
        }

        @Override
        protected @Nonnull EGQueryBuilder createEGQueryBuilder(int sizeHint) {
            return new APIEGQueryBuilder(sizeHint);
        }

        @Override
        protected @Nonnull EGQueryBuilder createEGQueryBuilder(@Nonnull CQuery parent) {
            return new APIEGQueryBuilder(parent);
        }

        private @Nonnull EGPrototype
        createEGPrototype(@Nonnull Collection<ImmutablePair<Term, Atom>> component) {
            Multimap<Term, Atom> term2atom = HashMultimap.create(parentQuery.size()*2, 8);
            for (ImmutablePair<Term, Atom> key : component)
                term2atom.put(key.left, key.right);

            Map<Triple, List<LinkMatch>> triple2matches = new HashMap<>();
            SetMultimap<Term, TermAnnotation> termAnnotations = HashMultimap.create();
            SetMultimap<Triple, TripleAnnotation> tripleAnnotations = HashMultimap.create();
            for (Term term : term2atom.keySet()) {
                CQuery query = subQueries.get(term);
                query.forEachTermAnnotation(termAnnotations::put);
                query.forEachTripleAnnotation(tripleAnnotations::put);
                for (Triple triple : query)
                    triple2matches.computeIfAbsent(triple, k -> new ArrayList<>());
                for (Atom atom : term2atom.get(term)) {
                    List<List<LinkMatch>> atomLists = visited.get(ImmutablePair.of(term, atom));
                    if (atomLists == null)
                        continue;
                    for (int i = 0; i < atomLists.size(); i++)
                        triple2matches.get(query.get(i)).addAll(atomLists.get(i));
                }
            }

            CQuery query = CQuery.builder(triple2matches.size())
                    .addAll(triple2matches.keySet())
                    .annotateAllTerms(termAnnotations)
                    .annotateAllTriples(tripleAnnotations).build();
            List<List<LinkMatch>> matchLists = new ArrayList<>(query.size());
            for (Triple triple : query)
                matchLists.add(triple2matches.get(triple));
            return new EGPrototype(query, matchLists);
        }

        @Override
        protected @Nonnull List<EGPrototype> getEGPrototypes() {
            List<EGPrototype> prototypes = new ArrayList<>();
            Set<ImmutablePair<Term, Atom>> visited2 = new HashSet<>();
            List<ImmutablePair<Term, Atom>> component = new ArrayList<>();
            ArrayDeque<ImmutablePair<Term, Atom>> stack = new ArrayDeque<>();
            for (ImmutablePair<Term, Atom> key : visited.keySet()) {
                stack.push(key);
                while (!stack.isEmpty()) {
                    ImmutablePair<Term, Atom> hop = stack.pop();
                    if (!visited2.add(hop)) continue;
                    component.add(hop);
                    incoming.get(hop).forEach(m -> stack.push(m.from));
                    List<List<LinkMatch>> matchesList = visited.get(hop);
                    if (matchesList != null)
                        matchesList.forEach(l -> l.forEach(m -> stack.push(m.getTo())));
                }
                if (!component.isEmpty()) {
                    EGPrototype prototype = createEGPrototype(component);
                    if (!prototype.matchLists.stream().allMatch(List::isEmpty))
                        prototypes.add(prototype);
                }
                component.clear();
                stack.clear();
            }
            return prototypes;
        }
    }
}
