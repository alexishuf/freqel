package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.lang.String.format;

@Immutable
public final class JoinType {
    private final boolean vars;
    private final @Nonnull Position from, to;

    public enum Position {
        SUBJ, PRED, OBJ, ANY;

        public boolean   hasSubject() { return this == SUBJ || this == ANY; }
        public boolean hasPredicate() { return this == PRED || this == ANY; }
        public boolean    hasObject() { return this ==  OBJ || this == ANY; }

        public boolean allows(@Nonnull Triple.Position position) {
            switch (this) {
                case SUBJ: return position == Triple.Position.SUBJ;
                case PRED: return position == Triple.Position.PRED;
                case  OBJ: return position == Triple.Position.OBJ;
                case  ANY: return true;
            }
            throw new UnsupportedOperationException("Cannot handle "+position);
        }
    }

    /* ~~~ singletons, constructor and builder ~~~*/

    public static final JoinType ANY       = JoinType.fromAny().to();
    public static final JoinType VARS      = JoinType.fromVar(Position.ANY ).to(Position.ANY );
    public static final JoinType SUBJ_SUBJ = JoinType.fromAny(Position.SUBJ).to(Position.SUBJ);
    public static final JoinType OBJ_SUBJ  = JoinType.fromAny(Position.OBJ ).to(Position.SUBJ);
    public static final JoinType OBJ_OBJ   = JoinType.fromAny(Position.OBJ ).to(Position.OBJ );
    public static final JoinType SUBJ_OBJ  = JoinType.fromAny(Position.SUBJ).to(Position.OBJ );

    private JoinType(boolean vars, @Nonnull Position from, @Nonnull Position to) {
        this.vars = vars;
        this.from = from;
        this.to = to;
    }

    public static class SecondStageBuilder {
        private boolean fromVars;
        private @Nonnull Position from;

        private SecondStageBuilder(boolean fromVars, @Nonnull Position from) {
            this.fromVars = fromVars;
            this.from = from;
        }

        public @Nonnull JoinType to(@Nonnull Position position) {
            return new JoinType(fromVars, from, position);
        }
        public @Nonnull JoinType to() { return to(Position.ANY); }
    }

    public static @Nonnull SecondStageBuilder fromAny(@Nonnull Position position) {
        return new SecondStageBuilder(false, position);
    }
    public static @Nonnull SecondStageBuilder fromVar(@Nonnull Position position) {
        return new SecondStageBuilder(true, position);
    }
    public static @Nonnull SecondStageBuilder fromAny() { return fromAny(Position.ANY); }
    public static @Nonnull SecondStageBuilder fromVar() { return fromVar(Position.ANY); }

    /* ~~~ getters ~~~*/

    public boolean requiresVars() { return vars; }
    public @Nonnull Position getFrom() { return from; }
    public @Nonnull Position getTo  () { return   to; }

    /* ~~~ strategy methods ~~~*/

    /**
     * Executes the consumer for each ({@link Term}, {@link Triple.Position}) pair in the
     * input triple that satifies this {@link JoinType} "from" policy.
     */
    public void forEachSourceAt(@Nonnull Triple triple,
                                @Nonnull BiConsumer<Term, Triple.Position> consumer) {
        if (from.hasSubject() && (!vars || triple.getSubject().isVar()))
            consumer.accept(triple.getSubject(), Triple.Position.SUBJ);
        if (from.hasPredicate() && (!vars || triple.getPredicate().isVar()))
            consumer.accept(triple.getPredicate(), Triple.Position.PRED);
        if (from.hasObject() && (!vars || triple.getObject().isVar()))
            consumer.accept(triple.getObject(), Triple.Position.OBJ);
    }

    /** Verifies if a triple satisfies the "to" policy assuming that the given joinTerm
     *   already satisfied the "from" poslicy. */
    public boolean allowDestination(@Nonnull Term joinTerm, @Nonnull Triple candidate) {
        Preconditions.checkArgument(joinTerm.isVar() || !vars,
                "\"from\" policy violation: joinTerm="+joinTerm+" is not a var");
        switch (to) {
            case SUBJ: return candidate.getSubject().equals(joinTerm);
            case PRED: return candidate.getPredicate().equals(joinTerm);
            case  OBJ: return candidate.getObject().equals(joinTerm);
            case  ANY: return candidate.contains(joinTerm);
        }
        throw new UnsupportedOperationException("Unexpected to="+to);
    }

    /**
     * If joinTerm, which appears at the given position in the source triple satisfies the
     * "from" policy, evaluate if candidate satisfies the "to" policy.
     *
     * @return true iff both the "from" and "to" policies are satisfied.
     */
    public boolean allowDestination(@Nonnull Term joinTerm, @Nonnull Triple.Position position,
                                    @Nonnull Triple candidate) {
        if (!from.allows(position) || (!joinTerm.isVar() && vars))
            return false; // "from" policy violated
        return allowDestination(joinTerm, candidate);
    }

    /** Same as <code>forEachSource()</code>, but for the "to" policy. */
    public void forEachDestinationAt(@Nonnull Triple triple,
                                     @Nonnull BiConsumer<Term, Triple.Position> consumer) {
        if (to.hasSubject() && (!vars || triple.getSubject().isVar()))
            consumer.accept(triple.getSubject(), Triple.Position.SUBJ);
        if (to.hasPredicate() && (!vars || triple.getPredicate().isVar()))
            consumer.accept(triple.getPredicate(), Triple.Position.PRED);
        if (to.hasObject() && (!vars || triple.getObject().isVar()))
            consumer.accept(triple.getObject(), Triple.Position.OBJ);
    }

    /* ~~~ object-ish methods ~~~*/

    @Override
    public @Nonnull String toString() {
        return format("%s%s-%s", from, vars ? "[var]":"", to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinType)) return false;
        JoinType joinType = (JoinType) o;
        return vars == joinType.vars &&
                from == joinType.from &&
                to == joinType.to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vars, from, to);
    }
}
