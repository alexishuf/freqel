package br.ufsc.lapesd.freqel.query.modifiers.filter;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.*;

@Immutable
public class SPARQLFilterNode {
    public static final @Nonnull Set<String> BI_OPS = Sets.newHashSet("<", "<=", "=", "==", "!=",
            ">=", ">", "+", "-", "*", "/",
            "&&", "||");

    public static final @Nonnull Set<String> L_BI_OPS = Sets.newHashSet("&&", "||");
    public static final @Nonnull Set<String> UN_OPS = Sets.newHashSet("!", "-", "+");
    public static final @Nonnull Set<String> L_UN_OPS = Collections.singleton("!");
    public static final @Nonnull Set<String> L_OPS = Sets.newHashSet("&&", "||", "!");

    private final @Nonnull String name;
    private final @Nonnull ImmutableList<SPARQLFilterNode> args;
    private final @Nullable Term term;
    private final @Nullable String string;
    private @Nullable @LazyInit Set<String> varsMentioned;

    public SPARQLFilterNode(@Nonnull String name, @Nonnull String string) {
        this.name = name;
        this.args = ImmutableList.of();
        this.term = null;
        this.string = string;
    }
    public SPARQLFilterNode(@Nonnull String name, @Nonnull List<SPARQLFilterNode> args) {
        this.name = name;
        this.args = ImmutableList.copyOf(args);
        this.term = null;
        this.string = null;
    }

    public SPARQLFilterNode(@Nonnull Term term) {
        this.name = term.toString();
        this.args = ImmutableList.of();
        this.term = term;
        this.string = null;
    }

    public @Nonnull String name() {
        return name;
    }

    public boolean isTerm() {
        return term != null;
    }

    public boolean isLogicalOp() {
        return !isTerm() && L_OPS.contains(name());
    }
    public boolean isLogicalBiOp() {
        return !isTerm() && L_BI_OPS.contains(name());
    }
    public boolean isLogicalUnOp() {
        return !isTerm() && L_UN_OPS.contains(name());
    }
    public boolean isBiOp() {
        return !isTerm() && BI_OPS.contains(name());
    }
    public boolean isUnOp() {
        return !isTerm() && UN_OPS.contains(name());
    }

    public @Nullable Term asTerm() {
        return term;
    }

    public int argsCount() {
        return args.size();
    }

    public @Nonnull ImmutableList<SPARQLFilterNode> args() {
        return args;
    }

    public @Nonnull SPARQLFilterNode arg(int i) {
        return args.get(i);
    }

    public @Nonnull Set<String> varsMentioned() {
        if (varsMentioned != null)
            return varsMentioned;
        if (isTerm()) {
            Term t = Objects.requireNonNull(asTerm());
            if (t.isVar())
                return varsMentioned = Collections.singleton(t.asVar().getName());
            return varsMentioned = Collections.emptySet();
        }
        Set<String> set = new HashSet<>();
        findVarsMentioned(set);
        return varsMentioned = set;
    }
    private void findVarsMentioned(@Nonnull Set<String> out) {
        if (isTerm()) {
            Term t = Objects.requireNonNull(asTerm());
            if (t.isVar())
                out.add(t.asVar().getName());
        } else {
            for (SPARQLFilterNode arg : args)
                arg.findVarsMentioned(out);
        }
    }

    @Override public @Nonnull String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }
    public @Nonnull String toString(@Nonnull PrefixDict prefixDict) {
        if (string != null)
            return string;
        if (term != null)
            return term.toString(prefixDict);

        StringBuilder b = new StringBuilder();
        if (isUnOp() && args.size() == 1) {
            return b.append(name())
                    .append(args.get(0).toString(prefixDict)).toString();
        } else if (isBiOp() && args.size() == 2) {
            return b.append('(').append(args.get(0).toString(prefixDict))
                    .append(' ').append(name()).append(' ')
                    .append(args.get(1).toString(prefixDict)).append(')').toString();
        } else {
            b.append(name()).append('(');
            for (SPARQLFilterNode arg : args)
                b.append(arg.toString(prefixDict)).append(", ");
            b.setLength(b.length() - (args.isEmpty() ? 0 : 2));
            return b.append(')').toString();
        }
    }
}
