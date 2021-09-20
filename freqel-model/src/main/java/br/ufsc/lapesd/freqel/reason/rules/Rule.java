package br.ufsc.lapesd.freqel.reason.rules;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.util.CollectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class Rule {
    private final @Nonnull List<Triple> consequent, antecedent;
    private int hash = 0;

    public Rule(@Nonnull List<Triple> consequent, @Nonnull List<Triple> antecedent) {
        this.consequent = consequent;
        this.antecedent = antecedent;
    }

    public static class Builder {
        private @Nullable List<Triple> consequent, antecedent;

        public @Nonnull Builder addConsequent(Triple triple) {
            (consequent == null ? consequent = new ArrayList<>() : consequent).add(triple);
            return this;
        }

        public @Nonnull Builder addAntecedent(Triple triple) {
            (antecedent == null ? antecedent = new ArrayList<>() : antecedent).add(triple);
            return this;
        }

        public @Nonnull Builder withConsequentList(@Nonnull List<Triple> list) {
            this.consequent = list;
            return this;
        }

        public @Nonnull Builder withAntecedentList(@Nonnull List<Triple> list) {
            this.antecedent = list;
            return this;
        }

        public @Nonnull Rule build() {
            List<Triple> antecedent = CollectionUtils.unmodifiableList(this.antecedent);
            List<Triple> consequent = CollectionUtils.unmodifiableList(this.consequent);
            return new Rule(antecedent, consequent);
        }
    }

    public @Nonnull List<Triple> consequent() {
        return consequent;
    }

    public @Nonnull List<Triple> antecedent() {
        return antecedent;
    }

    public @Nonnull Iterator<Triple> triplesIterator() {
        Iterator<Triple> conIt = consequent.iterator();
        Iterator<Triple> antIt = antecedent.iterator();
        return new Iterator<Triple>() {
            @Override public boolean hasNext() {
                return conIt.hasNext() || antIt.hasNext();
            }
            @Override public @Nonnull Triple next() {
                return conIt.hasNext() ? conIt.next() : antIt.next();
            }
        };
    }

    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder, @Nonnull Collection<Triple> triples, @Nonnull PrefixDict prefixDict) {
        for (Triple triple : triples)
            builder.append(triple.toString(prefixDict)).append(", ");
        if (triples.isEmpty())
            builder.setLength(builder.length()-2);
        return builder;
    }

    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        StringBuilder b = new StringBuilder((consequent.size()+antecedent.size())*20);
        return toString(toString(b, consequent, dict).append(" <- "), antecedent, dict).toString();
    }

    /* --- --- --- Object methods --- --- --- */

    @Override public @Nonnull String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Rule)) return false;
        Rule rule = (Rule) o;
        return hashCode() == o.hashCode() && consequent.equals(rule.consequent)
                                          && antecedent.equals(rule.antecedent);
    }

    @Override public int hashCode() {
        return hash == 0 ? (hash = Objects.hash(consequent, antecedent)) : hash;
    }
}
