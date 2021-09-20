package br.ufsc.lapesd.freqel.reason.rules;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.reason.tbox.IsTBoxTriple;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ImmIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static br.ufsc.lapesd.freqel.util.CollectionUtils.setMinus;
import static com.google.common.base.Preconditions.checkElementIndex;

public class RuleSet {
    private final @Nonnull ImmIndexSet<Rule> rules;
    private IndexSet<String> strings;
    private IndexSubset<String> predicates;
    private List<IndexSubset<Rule>> antecedentPredicate2rule;
    private List<IndexSubset<Rule>> consequentPredicate2rule;
    private IndexSubset<String> varNames;
    private List<IndexSubset<String>> rule2antecedentVars;
    private List<IndexSubset<String>> rule2consequentVars;
    private List<IndexSubset<String>> rule2antecedentPredicates;
    private List<IndexSubset<String>> rule2consequentPredicates;
    private List<IndexSubset<String>> rule2predicates;
    private IndexSubset<Rule> introducesBlankNodes;
    private IndexSubset<Rule> hasTBoxAntecedent;
    private IndexSubset<Rule> hasTBoxConsequent;
    private IndexSubset<Rule> singleABoxAntecedent;
    private IndexSubset<Rule> singleABoxConsequent;
    private IndexSubset<Rule> multiABoxAntecedent;
    private IndexSubset<Rule> multiABoxConsequent;

    private enum Side {
        ANT,
        CON;

        @Nonnull List<IndexSubset<Rule>> predicate2rule(@Nonnull RuleSet rs) {
            return this == ANT ? rs.antecedentPredicate2rule : rs.consequentPredicate2rule;
        }
        @Nonnull List<IndexSubset<String>> rule2var(@Nonnull RuleSet rs) {
            return this == ANT ? rs.rule2antecedentVars : rs.rule2consequentVars;
        }
        @Nonnull List<IndexSubset<String>> rule2predicate(@Nonnull RuleSet rs) {
            return this == ANT ? rs.rule2antecedentPredicates : rs.rule2consequentPredicates;
        }
        @Nonnull IndexSubset<Rule> hasTBox(@Nonnull RuleSet rs) {
            return this == ANT ? rs.hasTBoxAntecedent : rs.hasTBoxConsequent;
        }
        @Nonnull IndexSubset<Rule> singleABox(@Nonnull RuleSet rs) {
            return this == ANT ? rs.singleABoxAntecedent : rs.singleABoxConsequent;
        }
        @Nonnull IndexSubset<Rule> multiABox(@Nonnull RuleSet rs) {
            return this == ANT ? rs.multiABoxAntecedent : rs.multiABoxConsequent;
        }
        @Nonnull List<Triple> triples(@Nonnull Rule rule) {
            return this == ANT ? rule.antecedent() : rule.consequent();
        }
    }

    public RuleSet(@Nonnull Collection<Rule> collection) {
        this.rules = FullIndexSet.from(collection).asImmutable();
        this.strings = new FullIndexSet<>(rules.size()*3);
        this.varNames = strings.emptySubset();
        this.predicates = strings.emptySubset();
        this.rule2antecedentVars = new ArrayList<>(rules.size());
        this.rule2consequentVars = new ArrayList<>(rules.size());
        this.rule2antecedentPredicates = new ArrayList<>(rules.size());
        this.rule2consequentPredicates = new ArrayList<>(rules.size());
        this.rule2predicates = new ArrayList<>(rules.size());
        this.introducesBlankNodes = rules.emptySubset();
        this.hasTBoxAntecedent = rules.emptySubset();
        this.hasTBoxConsequent = rules.emptySubset();
        this.singleABoxAntecedent = rules.emptySubset();
        this.singleABoxConsequent = rules.emptySubset();
        this.multiABoxAntecedent = rules.emptySubset();
        this.multiABoxConsequent = rules.emptySubset();
        indexRules();
        indexByPredicates();
        makeImmutable();
    }

    /* --- --- --- Compare RuleSets --- --- --- */

    /**
     * Tests whether this set of rules entails everything that the other set also entails.
     *
     * Implementations are allowed to return false negatives in interest of efficient
     * implementations.
     *
     * @param other another {@link RuleSet} to compare against
     * @return true if every rule in other has a subsuming rule in this set.
     */
    public boolean subsumes(@Nonnull RuleSet other) {
        // Every rule not contained in this.rules must have a matching rule with
        // same antecedent and possibly larger consequent.
        // Isomorphism (wrt variable names) is not considered.
        for (Rule theirs : other.rules().fullSubset().minus(rules())) {
            ImmIndexSubset<String> aPreds = other.antecedentPredicates(theirs);
            ImmIndexSubset<String> cPreds = other.consequentPredicates(theirs);
            IndexSubset<Rule> set = withPredicateSignature(aPreds, cPreds);
            if (set.isEmpty())
                return false;
            List<Triple> ant = theirs.antecedent(), con = theirs.consequent();
            boolean match = false;
            for (Rule c : set) {
                match = c.antecedent().equals(ant) && c.consequent().containsAll(con);
                if (match) break;
            }
            if (!match)
                return false;
        }
        return true;
    }


    /* --- --- --- Object methods --- --- --- */

    @Override public String toString() {
        StringBuilder builder = new StringBuilder("RuleSet{\n");
        for (Rule rule : rules)
            builder.append(rule).append("\n");
        if (rules.isEmpty())
            builder.setLength(builder.length()-1);
        return builder.append("}").toString();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuleSet)) return false;
        RuleSet ruleSet = (RuleSet) o;
        return rules.equals(ruleSet.rules);
    }

    @Override public int hashCode() {
        return Objects.hash(rules);
    }

    /* --- --- --- Basic getters --- --- --- */

    public int size() { return rules.size(); }
    public @Nonnull ImmIndexSet<Rule> rules() { return rules; }

    /* --- --- --- Query indices --- --- --- */

    public @Nonnull ImmIndexSubset<Rule> withPredicate(@Nonnull Term predicate) {
        return !predicate.isURI() ? rules.immutableEmptySubset()
                                  : withPredicate(predicate.asURI().getURI());
    }
    public @Nonnull ImmIndexSubset<Rule> withPredicate(@Nonnull String uri) {
        ImmIndexSubset<Rule> a = withAntecedentPredicate(uri), c = withConsequentPredicate(uri);
        return a.isEmpty() ? c : (c.isEmpty() ? a : a.createImmutableUnion(c));
    }
    public @Nonnull ImmIndexSubset<Rule> withConsequentPredicate(@Nonnull Term predicate) {
        return predicate.isURI() ? withConsequentPredicate(predicate.asURI().getURI())
                                 : rules.immutableEmptySubset();
    }
    public @Nonnull ImmIndexSubset<Rule> withConsequentPredicate(@Nonnull String uri) {
        int idx = predicates.indexOf(uri);
        return idx >= 0 ? consequentPredicate2rule.get(idx).asImmutable()
                        : rules.immutableEmptySubset();
    }
    public @Nonnull ImmIndexSubset<Rule> withAntecedentPredicate(@Nonnull Term predicate) {
        return predicate.isURI() ? withAntecedentPredicate(predicate.asURI().getURI())
                : rules.immutableEmptySubset();
    }
    public @Nonnull ImmIndexSubset<Rule> withAntecedentPredicate(@Nonnull String uri) {
        int idx = predicates.indexOf(uri);
        return idx >= 0 ? antecedentPredicate2rule.get(idx).asImmutable()
                        : rules.immutableEmptySubset();
    }

    public @Nonnull IndexSubset<Rule>
    withPredicateSignature(@Nonnull IndexSet<String> antecedentPredicates,
                           @Nonnull IndexSet<String> consequentPredicates) {
        IndexSubset<Rule> set = this.rules.fullSubset();
        for (String p : antecedentPredicates)
            set.intersect(withAntecedentPredicate(p));
        for (String p : consequentPredicates)
            set.intersect(withConsequentPredicate(p));
        return set;
    }

    public @Nonnull ImmIndexSubset<String> antecedentVars(@Nonnull Rule rule) {
        return antecedentVars(rules.indexOf(rule));
    }
    public @Nonnull ImmIndexSubset<String> antecedentVars(int ruleIndex) {
        checkElementIndex(ruleIndex, size());
        return rule2antecedentVars.get(ruleIndex).asImmutable();
    }
    public @Nonnull ImmIndexSubset<String> consequentVars(@Nonnull Rule rule) {
        return consequentVars(rules.indexOf(rule));
    }
    public @Nonnull ImmIndexSubset<String> consequentVars(int ruleIndex) {
        checkElementIndex(ruleIndex, size());
        return rule2consequentVars.get(ruleIndex).asImmutable();
    }
    public @Nonnull ImmIndexSet<String> vars(@Nonnull Rule rule) {
        return vars(rules.indexOf(rule));
    }
    public @Nonnull ImmIndexSet<String> vars(int ruleIndex) {
        checkElementIndex(ruleIndex, size());
        IndexSubset<String> a = rule2antecedentVars.get(ruleIndex),
                            c = rule2consequentVars.get(ruleIndex);
        return a.isEmpty() ? c.asImmutable()
                           : (c.isEmpty() ? a.asImmutable() : a.createImmutableUnion(c));
    }

    public @Nonnull ImmIndexSubset<String> antecedentPredicates(@Nonnull Rule rule) {
        return antecedentPredicates(rules.indexOf(rule));
    }
    public @Nonnull ImmIndexSubset<String> antecedentPredicates(int ruleIndex) {
        checkElementIndex(ruleIndex, size());
        return rule2antecedentPredicates.get(ruleIndex).asImmutable();
    }
    public @Nonnull ImmIndexSubset<String> consequentPredicates(@Nonnull Rule rule) {
        return consequentPredicates(rules.indexOf(rule));
    }
    public @Nonnull ImmIndexSubset<String> consequentPredicates(int ruleIndex) {
        checkElementIndex(ruleIndex, size());
        return rule2consequentPredicates.get(ruleIndex).asImmutable();
    }
    public @Nonnull ImmIndexSubset<String> predicates(@Nonnull Rule rule) {
        return predicates(rules.indexOf(rule));
    }
    public @Nonnull ImmIndexSubset<String> predicates(int ruleIndex) {
        checkElementIndex(ruleIndex, size());
        return rule2predicates.get(ruleIndex).asImmutable();
    }

    public @Nonnull ImmIndexSubset<Rule> introducesBlankNodes() {
        return introducesBlankNodes.asImmutable();
    }
    public @Nonnull IndexSubset<Rule> getHasTBoxAntecedent() {
        return hasTBoxAntecedent.asImmutable();
    }
    public @Nonnull IndexSubset<Rule> getHasTBoxConsequent() {
        return hasTBoxConsequent.asImmutable();
    }
    public @Nonnull IndexSubset<Rule> getSingleABoxAntecedent() {
        return singleABoxAntecedent.asImmutable();
    }
    public @Nonnull IndexSubset<Rule> getSingleABoxConsequent() {
        return singleABoxConsequent.asImmutable();
    }
    public @Nonnull IndexSubset<Rule> getMultiABoxAntecedent() {
        return multiABoxAntecedent.asImmutable();
    }
    public @Nonnull IndexSubset<Rule> getMultiABoxConsequent() {
        return multiABoxConsequent.asImmutable();
    }

    /* --- --- --- Rule indexing --- --- --- */

    private void indexRules() {
        for (int i = 0, size = rules.size(); i < size; i++) {
            Rule rule = rules.get(i);
            indexRules(Side.ANT, rule, i);
            indexRules(Side.CON, rule, i);
            if (!setMinus(rule2consequentVars.get(i), rule2antecedentVars.get(i)).isEmpty())
                introducesBlankNodes.setIndex(i, rules);
            rule2predicates.add(rule2antecedentPredicates.get(i)
                    .createUnion(rule2consequentPredicates.get(i))
                    .asImmutable());
        }
    }

    protected void indexRules(@Nonnull Side side, @Nonnull Rule rule, int ruleIndex) {
        assert strings != null && varNames != null && predicates != null;
        IndexSubset<String> sideVars = strings.emptySubset();
        IndexSubset<String> sidePredicates = strings.emptySubset();
        boolean hasTBox = false;
        int aBox = 0;
        for (Triple triple : side.triples(rule)) {
            triple.forEach(term -> {
                String name = term.isVar() ? term.asVar().getName()
                                           : (term.isBlank() ? term.asBlank().getName() : null);
                if (name != null) {
                    int idx = strings.indexOfAdd(name);
                    varNames.setIndex(idx, strings);
                    sideVars.setIndex(idx, strings);
                }
            });
            Term predicate = triple.getPredicate();
            if (predicate.isURI()) {
                int idx = strings.indexOfAdd(predicate.asURI().getURI());
                predicates.setIndex(idx, strings);
                sidePredicates.setIndex(idx, strings);
            }
            if (IsTBoxTriple.INSTANCE.test(triple)) hasTBox = true;
            else                                    ++aBox;
        }

        List<IndexSubset<String>> rule2var = side.rule2var(this);
        assert rule2var.size() == ruleIndex;
        rule2var.add(sideVars);

        List<IndexSubset<String>> rule2predicate = side.rule2predicate(this);
        assert rule2predicate.size() == ruleIndex;
        rule2predicate.add(sidePredicates);

        if (hasTBox)
            side.hasTBox(this).setIndex(ruleIndex, rules);
        if (aBox == 1)
            side.singleABox(this).setIndex(ruleIndex, rules);
        else if (aBox > 1)
            side.multiABox(this).setIndex(ruleIndex, rules);
    }


    protected void indexByPredicates(@Nonnull Side side, @Nonnull Rule rule, int ruleIdx) {
        for (Triple triple : side.triples(rule)) {
            Term predicate = triple.getPredicate();
            if (!predicate.isURI()) continue;
            int idx = predicates.indexOf(predicate.asURI().getURI());
            side.predicate2rule(this).get(idx).setIndex(ruleIdx, rules);
        }
    }

    private <T> void makeImmutable(@Nonnull List<IndexSubset<T>> list) {
        for (int i = 0, size = list.size(); i < size; i++)
            list.set(i, list.get(i).asImmutable());
    }

    private void makeImmutable() {
        makeImmutable(rule2antecedentVars);
        makeImmutable(rule2consequentVars);
        makeImmutable(rule2antecedentPredicates);
        makeImmutable(rule2consequentPredicates);
        makeImmutable(antecedentPredicate2rule);
        makeImmutable(consequentPredicate2rule);
        introducesBlankNodes = introducesBlankNodes.asImmutable();
        hasTBoxAntecedent = hasTBoxAntecedent.asImmutable();
        hasTBoxConsequent = hasTBoxConsequent.asImmutable();
        singleABoxAntecedent = singleABoxAntecedent.asImmutable();
        singleABoxConsequent = singleABoxConsequent.asImmutable();
        multiABoxAntecedent = multiABoxAntecedent.asImmutable();
        multiABoxConsequent = multiABoxConsequent.asImmutable();
    }

    private void indexByPredicates() {
        int nPredicates = predicates.size();
        antecedentPredicate2rule = new ArrayList<>(nPredicates);
        consequentPredicate2rule = new ArrayList<>(nPredicates);
        for (int i = 0; i < nPredicates; i++) {
            antecedentPredicate2rule.add(rules.emptySubset());
            consequentPredicate2rule.add(rules.emptySubset());
        }
        for (int i = 0, size = rules.size(); i < size; i++) {
            Rule rule = rules.get(i);
            indexByPredicates(Side.ANT, rule, i);
            indexByPredicates(Side.CON, rule, i);
        }
    }

}
