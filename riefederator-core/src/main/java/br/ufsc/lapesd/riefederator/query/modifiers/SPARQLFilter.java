package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.jena.model.term.node.JenaVarNode;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.util.ArraySet;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.jena.ExprUtils.*;
import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.toJenaNode;
import static java.util.Collections.emptySet;
import static org.apache.jena.sparql.sse.Tags.*;

@Immutable
public class SPARQLFilter implements Modifier {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLFilter.class);

    private final @Nonnull String filter;
    private final @SuppressWarnings("Immutable") @Nonnull Expr expr;
    private @LazyInit int hash = 0;
    private @SuppressWarnings("Immutable") @Nullable @LazyInit Set<Var> vars = null;
    private @SuppressWarnings("Immutable") @Nullable @LazyInit Set<String> varNames = null;
    private @SuppressWarnings("Immutable") @Nullable @LazyInit Set<Term> terms = null;

    /* --- --- --- Constructor & builder --- --- --- */

    SPARQLFilter(@Nonnull String filter, @Nonnull Expr expr) {
        assert !filter.matches("(?i)^\\s*FILTER.*");
        this.filter = filter;
        this.expr = expr;
    }

    public static @Nonnull SPARQLFilter build(@Nonnull String filter) {
        return new  SPARQLFilter(innerExpression(filter), parseFilterExpr(innerExpression(filter)));
    }
    public static @Nonnull SPARQLFilter build(@Nonnull Expr expr) {
        return new  SPARQLFilter(toSPARQLSyntax(expr), expr);
    }

    public @Nonnull SPARQLFilter withVarsEvaluatedAsUnbound(@Nonnull Collection<String> varTermNames) {
        Set<String> set = varTermNames instanceof  Set ? (Set<String>)varTermNames
                : (varTermNames.size() > 4 ? new HashSet<>(varTermNames)
                                           : ArraySet.fromDistinct(varTermNames));
        Expr expr = ExprTransformer.transform(new ExprTransformCopy(false) {
            @Override
            public Expr transform(ExprFunction1 func, Expr e1) {
                String fn = func.getFunctionName(new SerializationContext()).toLowerCase();
                if (fn.equals("bound") && e1.isVariable() && set.contains(e1.getVarName()))
                    return NodeValue.FALSE;
                return super.transform(func, e1);
            }
        }, this.expr);

        if (this.expr == expr)
            return this;
        return build(expr);
    }

    /* --- --- --- Interface --- --- --- */

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.SPARQL_FILTER;
    }

    /* --- --- --- Getters --- --- --- */

    public @Nonnull Expr getExpr() {
        return expr;
    }

    public boolean isTrivial() {
        return getVarNames().isEmpty();
    }

    public @Nullable Boolean getTrivialResult() {
        if (!isTrivial())
            return null;
        return new SPARQLFilterExecutor().evaluate(this, ArraySolution.EMPTY);
    }

    public @Nonnull String getFilterString() {
        return filter;
    }

    public @Nonnull String getSparqlFilter() {
        return "FILTER(" + getFilterString() + ")";
    }

    public @Nonnull Set<Term> getTerms() {
        if (terms == null) {
            int capacity = varNames != null ? varNames.size() : (vars != null ? vars.size() : 10);
            Set<Term> set = Sets.newHashSetWithExpectedSize(capacity);
            expr.visit(new ExprVisitorBase() {
                private void visitFunction(ExprFunction func) {
                    for (int i = 1; i <= func.numArgs(); i++)
                        func.getArg(i).visit(this);
                }
                @Override
                public void visit(ExprFunction0 func) {  visitFunction(func); }
                @Override
                public void visit(ExprFunction1 func) {  visitFunction(func); }
                @Override
                public void visit(ExprFunction2 func) {  visitFunction(func); }
                @Override
                public void visit(ExprFunction3 func) {  visitFunction(func); }
                @Override
                public void visit(ExprFunctionN func) {  visitFunction(func); }
                @Override
                public void visit(ExprFunctionOp op) { visitFunction(op); }

                @Override
                public void visit(NodeValue nv) {
                    set.add(fromJena(nv.asNode()));
                }

                @Override
                public void visit(ExprVar nv) {
                    set.add(new JenaVarNode((Node_Variable) nv.getAsNode()));
                }
                @Override
                public void visit(ExprAggregator eAgg) {/* pass */}
                @Override
                public void visit(ExprNone exprNone) {/* pass */}
            });
            terms = set;
        }
        return terms;
    }

    public @Nonnull Set<Var> getVars() {
        if (vars == null) {
            Set<org.apache.jena.sparql.core.Var> jenaVars = expr.getVarsMentioned();
            HashSet<Var> set = Sets.newHashSetWithExpectedSize(jenaVars.size());
            for (org.apache.jena.sparql.core.Var v : jenaVars)
                set.add(new JenaVarNode(v));
            vars = set;
        }
        return vars;
    }

    public @Nonnull Set<String> getVarNames() {
        if (varNames == null) {
            Set<String> set;
            if (vars != null) {
                set = Sets.newHashSetWithExpectedSize(vars.size());
                for (Var v : vars) set.add(v.getName());
            } else {
                Set<org.apache.jena.sparql.core.Var> jenaVars = expr.getVarsMentioned();
                set = Sets.newHashSetWithExpectedSize(jenaVars.size());
                for (org.apache.jena.sparql.core.Var v : jenaVars) set.add(v.getVarName());
            }
            varNames = set;
        }
        return varNames;
    }

    /* --- --- --- Subsummption --- --- --- */

    private static class SubsumedVisitor implements ExprVisitor {
        private static final @Nonnull ImmutableSet<String> REL_SYMBOLS
                = ImmutableSet.of(tagLT, tagLE, tagEQ, tagGE, tagGT);
        private @Nonnull Expr left, right;
        private @Nonnull Expr leftRoot, rightRoot;
        private @Nonnull SPARQLFilter leftFilter, rightFilter;
        boolean subsumed = true, onRight = false;
        Map<Term, Term> subsumed2subsumer = new HashMap<>();

        public SubsumedVisitor(@Nonnull SPARQLFilter left, @Nonnull SPARQLFilter right) {
            this.leftFilter = left;
            this.rightFilter = right;
            this.leftRoot  = this.left  = left.expr;
            this.rightRoot = this.right = right.expr;
        }

        public SubsumptionResult areResultsSubsumedBy() {
            left.visit(this);
            if (!subsumed) return new SubsumptionResult(leftFilter, rightFilter, null);
            ImmutableBiMap.Builder<Term, Term> builder = ImmutableBiMap.builder();
            subsumed2subsumer.entrySet().forEach(builder::put);
            return new SubsumptionResult(leftFilter, rightFilter, builder.build());
        }

        private void visitRight() {
            onRight = true;
            right.visit(this);
            onRight = false;
        }

        private @Nonnull ExprFunction normalizeConstant(@Nonnull ExprFunction f) {
            String sym = f.getFunctionSymbol().getSymbol();
            if (!REL_SYMBOLS.contains(sym))
                return f;
            Expr l = f.getArg(1), r = f.getArg(2);
            if (f.numArgs() != 2)
                throw new IllegalArgumentException("expected 2 args for "+sym+", got"+f.numArgs());
            if (l instanceof NodeValue && !(r instanceof NodeValue)) {
                switch (sym) {
                    case tagGE:
                        return new E_LessThan(r, l);
                    case tagGT:
                        return new E_LessThanOrEqual(r, l);
                    case tagLE:
                        return new E_GreaterThan(r, l);
                    case tagLT:
                        return new E_GreaterThanOrEqual(r, l);
                }
            }
            return f; // already normalized
        }

        private @Nullable Term toTerm(@Nonnull Expr expr) {
            if (expr instanceof NodeValue) {
                return JenaWrappers.fromJena(((NodeValue)expr).asNode());
            } else if (expr instanceof ExprVar) {
                return new StdVar(expr.getVarName());
            } else {
                return null;
            }
        }

        private void tryMap(@Nonnull Expr subsumed, @Nonnull Expr subsumer) {
            if (!this.subsumed) return;
            Term subsumedTerm = toTerm(subsumed);
            Term subsumerTerm = toTerm(subsumer);
            if (subsumedTerm != null && subsumerTerm != null) {
                Term old = subsumed2subsumer.put(subsumedTerm, subsumerTerm);
                if (old != null && !subsumerTerm.equals(old) && subsumedTerm.isVar()) {
                    logger.warn("Variable {} of possible subsumed filter {} mapped to " +
                                "both {} and {} in possible subsumer filter {}",
                                subsumedTerm, leftRoot, old, subsumerTerm, rightRoot);
                }
            }
        }

        private boolean processRelational() {
            assert left instanceof ExprFunction;
            assert right instanceof ExprFunction;
            ExprFunction lFunc = (ExprFunction) this.left, rFunc = (ExprFunction) this.right;
            String lSym = lFunc.getFunctionSymbol().getSymbol(),
                   rSym = rFunc.getFunctionSymbol().getSymbol();
            if (!REL_SYMBOLS.contains(lSym) && REL_SYMBOLS.contains(rSym))
                return false;  //recurse to compare operands
            lFunc = normalizeConstant(lFunc);
            rFunc = normalizeConstant(rFunc);
            lSym = lFunc.getFunctionSymbol().getSymbol();
            rSym = rFunc.getFunctionSymbol().getSymbol();

            if (!(lFunc.getArg(2) instanceof NodeValue)
                    || (lFunc.getArg(1) instanceof NodeValue)
                    || !(rFunc.getArg(2) instanceof NodeValue)
                    || (rFunc.getArg(1) instanceof NodeValue)) {
                // not normal form (?x op const)
                return false; //recurse
            }
            // normal form: ?x op const for both lFunc and rFunc
            NodeValue lv = (NodeValue)lFunc.getArg(2), rv = (NodeValue)rFunc.getArg(2);
            boolean lG = lSym.equals(tagGE) || lSym.equals(tagGT) || lSym.equals(tagEQ);
            boolean lL = lSym.equals(tagLE) || lSym.equals(tagLT) || lSym.equals(tagEQ);
            if (rSym.equals(tagGT) || rSym.equals(tagGE)) {
                if (lG && (lSym.equals(rSym) || rSym.equals(tagGE)))
                    subsumed &= NodeValue.compare(rv, lv) <= 0;
                else if (rSym.equals(tagGT) && (lSym.equals(tagGE) || lSym.equals(tagEQ)))
                    subsumed &= NodeValue.compare(rv, lv) < 0;
                else
                    subsumed = false;
                tryMap(lFunc.getArg(1), rFunc.getArg(1));
                tryMap(lv, rv);
                return true; //complete: do not recurse
            } else if (rSym.equals(tagLT) || rSym.equals(tagLE)) {
                if (lL && (lSym.equals(rSym) || rSym.equals(tagLE)))
                    subsumed &= NodeValue.compare(rv, lv) >= 0;
                else if (rSym.equals(tagLT) && (lSym.equals(tagLE) || lSym.equals(tagEQ)))
                    subsumed &= NodeValue.compare(rv, lv) > 0;
                else
                    subsumed = false;
                tryMap(lFunc.getArg(1), rFunc.getArg(1));
                tryMap(lv, rv);
                return true; //complete: do not recurse
            }
            return false; //recurse
        }

        private void visitFunction(ExprFunction func) {
            if (onRight) {
                FunctionLabel rSym = func.getFunctionSymbol();
                subsumed &= left instanceof ExprFunction;
                if (!subsumed)
                    return; // already failed
                FunctionLabel lSym = ((ExprFunction) left).getFunctionSymbol();
                String lStr = lSym.getSymbol(), rStr = rSym.getSymbol();
                subsumed &= Objects.equals(lStr, rStr)
                        || (rStr.equals(tagGE) && (lStr.equals(tagGT) || lStr.equals(tagEQ)))
                        || (rStr.equals(tagLE) && (lStr.equals(tagLT) || lStr.equals(tagEQ)));
            } else {
                assert left == func;
                if (right instanceof ExprFunction && processRelational())
                    return; // no more work
                // compare for equality
                visitRight();
                if (subsumed) {
                    ExprFunction rightFunc = (ExprFunction) this.right;
                    assert rightFunc.numArgs() == func.numArgs();
                    for (int i = 1; subsumed && i <= func.numArgs(); i++) {
                        left = func.getArg(i);
                        right = rightFunc.getArg(i);
                        left.visit(this); //will call visitRight
                    }
                }
            }
        }

        @Override
        public void visit(ExprFunction0 func) {
            visitFunction(func);
        }

        @Override
        public void visit(ExprFunction1 func) {
            visitFunction(func);
        }

        @Override
        public void visit(ExprFunction2 func) {
            visitFunction(func);
        }

        @Override
        public void visit(ExprFunction3 func) {
            visitFunction(func);
        }

        @Override
        public void visit(ExprFunctionN func) {
            visitFunction(func);
        }

        public void visitForEquality(Expr expr) {
            if (onRight) {
                assert expr == right;
                subsumed &= Objects.equals(left, expr);
            } else {
                left = expr;
                visitRight();
            }
        }

        @Override
        public void visit(ExprFunctionOp funcOp) {
            visitForEquality(funcOp);
        }

        @Override
        public void visit(NodeValue nv) {
            visitForEquality(nv);
            if (onRight)
                tryMap(left, right);
        }

        @Override
        public void visit(ExprVar nv) {
            if (onRight) {
                assert right == nv;
                subsumed &= left instanceof ExprVar || left instanceof NodeValue;
                tryMap(left, right);
            } else {
                left = nv;
                visitRight();
            }
        }

        @Override
        public void visit(ExprAggregator eAgg) {
            visitForEquality(eAgg);
        }

        @Override
        public void visit(ExprNone exprNone) {
            visitForEquality(exprNone);
        }
    }

    @Immutable
    public static class SubsumptionResult {
        private final @Nullable ImmutableBiMap<Term, Term> subsumed2subsuming;
        private final @Nonnull SPARQLFilter subsumed, subsumer;

        public SubsumptionResult(@Nonnull SPARQLFilter subsumed, @Nonnull SPARQLFilter subsumer,
                                 @Nullable BiMap<Term, Term> biMap) {
            this.subsumed = subsumed;
            this.subsumer = subsumer;
            this.subsumed2subsuming = biMap instanceof ImmutableBiMap
                    ? (ImmutableBiMap<Term, Term>)biMap
                    : (biMap == null ? null : ImmutableBiMap.copyOf(biMap));
        }

        public boolean getValue() {
            return subsumed2subsuming != null;
        }

        public @Nonnull SPARQLFilter getSubsumed() {
            return subsumed;
        }

        public @Nonnull SPARQLFilter getSubsumer() {
            return subsumer;
        }

        public @Nonnull Set<Term> subsumedTerms() {
            return subsumed2subsuming == null ? emptySet() : subsumed2subsuming.keySet();
        }
        public @Nonnull Set<Term> subsumingTerms() {
            return subsumed2subsuming == null ? emptySet() : subsumed2subsuming.inverse().keySet();
        }

        public @Nullable Term getOnSubsumed(@Nonnull Term onSubsumed) {
            return subsumed2subsuming == null ? null : subsumed2subsuming.inverse().get(onSubsumed);
        }
        public @Nullable Term getOnSubsuming(@Nonnull Term onSubsumer) {
            return subsumed2subsuming == null ? null : subsumed2subsuming.get(onSubsumer);
        }

        @Override
        public String toString() {
            return subsumed2subsuming == null ? "NOT_SUBSUMES" : subsumed2subsuming.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SubsumptionResult)) return false;
            SubsumptionResult that = (SubsumptionResult) o;
            return Objects.equals(subsumed2subsuming, that.subsumed2subsuming) &&
                    getSubsumed().equals(that.getSubsumed()) &&
                    getSubsumer().equals(that.getSubsumer());
        }

        @Override
        public int hashCode() {
            return Objects.hash(subsumed2subsuming, getSubsumed(), getSubsumer());
        }
    }

    /**
     * Returns true iff all results that pass evalution of this filter also pass for other.
     */
    public SubsumptionResult areResultsSubsumedBy(@Nonnull SPARQLFilter other) {
        return new SubsumedVisitor(this, other).areResultsSubsumedBy();
    }

    /* --- --- --- Variable Binding --- --- --- */

    public @Nonnull SPARQLFilter bind(@Nonnull Solution solution) {
        if (getVars().stream().noneMatch(v -> solution.getVarNames().contains(v.getName())))
            return this; // no change
        Expr boundExpr = this.expr.applyNodeTransform(n -> {
            if (!n.isVariable())
                return n;
            Node bound = toJenaNode(solution.get(n.getName()));
            return bound == null ? n : bound;
        });
        return build(boundExpr);
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SPARQLFilter)) return false;
        SPARQLFilter that = (SPARQLFilter) o;
        return expr.equals(that.expr);
    }

    @Override
    public int hashCode() {
        return hash == 0 ? hash = expr.hashCode() : hash;
    }

    @Override
    public String toString() {
        return "FILTER(" + filter + ")";
    }
}
