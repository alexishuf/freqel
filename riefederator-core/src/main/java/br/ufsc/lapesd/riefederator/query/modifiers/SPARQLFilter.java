package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.util.ArraySet;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import com.google.common.collect.*;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.riefederator.jena.ExprUtils.toSPARQLSyntax;
import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.toJenaNode;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.apache.jena.sparql.sse.Tags.*;

@Immutable
public class SPARQLFilter implements Modifier {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLFilter.class);
    private static final Pattern varNameRx =
            Pattern.compile("(?:^|\\s|[,(.])[$?]([^ \t.),\n]+)(?:\\s|[,.)]|$)");
    private static final Set<String> biOps = Sets.newHashSet("<", "<=", "=", "==", "!=", ">=", ">",
                                                             "+", "-", "*", "/",
                                                             "&&", "||");
    private static final Set<String> unOps = Sets.newHashSet("!", "-", "+");

    private final @Nonnull String filter;
    private final @SuppressWarnings("Immutable") @Nonnull Expr expr;
    private final @Nonnull ImmutableBiMap<String, Term> var2term;
    private @SuppressWarnings("Immutable") @Nonnull @LazyInit SoftReference<Set<Var>> vars
            = new SoftReference<>(null);
    private @SuppressWarnings("Immutable") @Nonnull @LazyInit SoftReference<Set<String>> varNames
            = new SoftReference<>(null);
    private @SuppressWarnings("Immutable") @Nonnull @LazyInit SoftReference<Set<Term>> terms
            = new SoftReference<>(null);

    /* --- --- --- Constructor & builder --- --- --- */

    SPARQLFilter(@Nonnull String filter, @Nonnull Expr expr,
                 @NotNull ImmutableBiMap<String, Term> var2term) {
        this.filter = innerExpression(filter);
        this.expr = expr;
        //noinspection Convert2MethodRef
        Set<String> actualNames = this.expr.getVarsMentioned().stream()
                .map(v -> v.getVarName()).collect(toSet());
        if (var2term.isEmpty()) {
            HashBiMap<String, Term> v2t = HashBiMap.create();
            for (String name : actualNames)
                v2t.put(name, new StdVar(name));
            var2term = ImmutableBiMap.copyOf(v2t);
        } else if (!var2term.keySet().equals(actualNames)) {
            if (!actualNames.containsAll(var2term.keySet())) {
                logger.warn("var2term map mentions variables not appearing in the expression: {}",
                        CollectionUtils.setMinus(actualNames, var2term.keySet()));
            }
            if (!var2term.keySet().containsAll(actualNames)) {
                logger.info("var2term misses some variables in query. Will map to homonyms: {}",
                        CollectionUtils.setMinus(var2term.keySet(), actualNames));
            }
            HashBiMap<String, Term> v2t = HashBiMap.create(var2term);
            for (String name : actualNames) {
                if (!v2t.containsKey(name))
                    v2t.put(name, new StdVar(name));
            }
            var2term = ImmutableBiMap.copyOf(v2t);
        }
        this.var2term = var2term;
    }

    /**
     * Create a new instance from the filter expression (e.g., "x" in "FILTER(x)")
     * mapping variable names (not including the $ and ? markers) to {@link Term}s.
     *
     * @param filter filter expression. Both "FILTER(x)" and "x" are accepted provided FILTER(x)
     *               is a valid SPARQL filter.
     * @param var2term A map from variable names in the filter expression to {@link Term}s.
     *                 The mapping must be bijective.
     */
    public SPARQLFilter(@Nonnull String filter,
                        @Nonnull ImmutableBiMap<String, Term> var2term) {
        this(innerExpression(filter), parseFilterExpr(innerExpression(filter)), var2term);
    }

    private static @Nonnull String innerExpression(@Nonnull String string) {
        return string.replaceAll("(?i)^\\s*FILTER\\((.*)\\)\\s*$", "$1");
    }

    private static @Nonnull Expr parseFilterExpr(@Nonnull String string) {
        String inner = string.replaceAll("(?i)^\\s*FILTER\\((.*)\\)\\s*$", "$1");
        StringBuilder b = new StringBuilder();
        StdPrefixDict.STANDARD.forEach((name, uri)
                -> b.append("PREFIX ").append(name).append(": <").append(uri).append(">\n"));
        b.append("SELECT * WHERE {\n");
        Matcher matcher = varNameRx.matcher(string);
        while (matcher.find())
            b.append("  ?s ?p $").append(matcher.group(1)).append(".\n");
        try {
            String sparql = b.append("  FILTER(").append(inner).append(")\n}\n").toString();
            Query query = QueryFactory.create(sparql);
            Expr[] expr = {null};
            query.getQueryPattern().visit(new ElementVisitorBase() {
                @Override
                public void visit(ElementFilter el) {
                    expr[0] = el.getExpr();
                }

                @Override
                public void visit(ElementGroup el) {
                    el.getElements().forEach(e -> e.visit(this));
                }
            });
            if (expr[0] == null) {
                throw new RuntimeException("Jena found no FILTER Expr in query. " +
                        "Most likely the parser changed its parse and this code is now broken.");
            }
            return expr[0];
        } catch (QueryException e) {
            throw new FilterParsingException(inner, e);
        }
    }

    public static class Builder {
        protected @Nullable String filter;
        protected @Nullable Expr expr;
        protected @Nonnull BiMap<String, Term> var2term = HashBiMap.create();

        public Builder(@Nonnull String filter) {
            this.filter = filter;
        }

        public Builder(@Nonnull Expr expr) {
            this.expr = expr;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder map(@Nonnull String var, @Nonnull Term term) {
            Term old = var2term.getOrDefault(var, null);
            if (old != null && old != term)
                logger.warn("mapping same var name ({}) to two terms: {}, {}", var, old, term);
            var2term.put(var, term);
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder map(@Nonnull Var var) {
            return map(var.getName(), var);
        }

        @CheckReturnValue
        public @Nonnull SPARQLFilter build() {
            if (filter != null) {
                assert expr == null;
                return new SPARQLFilter(filter, ImmutableBiMap.copyOf(var2term));
            } else if (expr != null) {
                String sparql = toSPARQLSyntax(expr);
                return new SPARQLFilter(sparql, expr, ImmutableBiMap.copyOf(var2term));
            } else {
                throw new IllegalStateException("Builder has neither filter nor expr");
            }
        }
    }

    public static @Nonnull Builder builder(@Nonnull String filter) {
        return new Builder(filter);
    }
    public static @Nonnull Builder builder(@Nonnull Expr expr) {
        return new Builder(expr);
    }
    public static @Nonnull SPARQLFilter build(@Nonnull String filter) {
        return new  SPARQLFilter(filter, ImmutableBiMap.of() );
    }
    public static @Nonnull SPARQLFilter build(@Nonnull Expr expr) {
        return new  SPARQLFilter(toSPARQLSyntax(expr), expr, ImmutableBiMap.of());
    }

    public @Nonnull SPARQLFilter withVarTermsUnbound(@Nonnull Collection<String> varTermNames) {
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
        Builder builder = SPARQLFilter.builder(expr);
        for (Map.Entry<String, Term> e : var2term.entrySet()) {
            if (e.getValue().isVar() && set.contains(e.getValue().asVar().getName()))
                continue;
            builder.map(e.getKey(), e.getValue());
        }
        return builder.build();
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
        return getVars().isEmpty();
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

    public @Nonnull ImmutableBiMap<String, Term> getVar2Term() {
        return var2term;
    }

    public @Nullable Term get(@Nonnull String var) {
        return var2term.get(var);
    }

    public @Nonnull Set<String> getVars() {
        return var2term.keySet();
    }

    public @Nonnull Set<Term> getTerms() {
        Set<Term> strong = this.terms.get();
        if (strong == null) {
            HashSet<Term> finalSet = new HashSet<>(var2term.size() * 2);
            finalSet.addAll(var2term.values());
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
                    finalSet.add(fromJena(nv.asNode()));
                }

                @Override
                public void visit(ExprVar nv) { /* pass */ }
                @Override
                public void visit(ExprAggregator eAgg) {/* pass */}
                @Override
                public void visit(ExprNone exprNone) {/* pass */}
            });
            this.terms = new SoftReference<>(strong = finalSet);
        }
        return strong;
    }

    public @Nonnull Set<Var> getVarTerms() {
        Set<Var> strong = this.vars.get();
        if (strong == null) {
            strong = getTerms().stream().filter(v -> v instanceof Var)
                                        .map(v -> (Var)v).collect(toSet());
            this.vars = new SoftReference<>(strong);
        }
        return strong;
    }

    public @Nonnull Set<String> getVarTermNames() {
        Set<String> strong = this.varNames.get();
        if (strong == null) {
            strong = getVarTerms().stream().map(Var::getName).collect(toSet());
            this.varNames = new SoftReference<>(strong);
        }
        return strong;
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
        if (getVarTerms().stream().noneMatch(v -> solution.getVarNames().contains(v.getName())))
            return this; // no change

        ImmutableBiMap<String, Term> v2t = getVar2Term();
        Set<String> victims = new HashSet<>(v2t.keySet().size());
        Expr boundExpr = this.expr.applyNodeTransform(n -> {
            if (!n.isVariable())
                return n;

            Term term = v2t.get(n.getName());
            assert term != null;
            if (!term.isVar()) {
                victims.add(n.getName());
                return toJenaNode(term);
            }

            Node node = toJenaNode(solution.get(term.asVar().getName()));
            if (node != null) {
                victims.add(n.getName());
                return node;
            }
            return n;
        });

        ImmutableBiMap.Builder<String, Term> builder = ImmutableBiMap.builder();
        for (Map.Entry<String, Term> e : v2t.entrySet()) {
            String varName = e.getKey();
            if (!victims.contains(varName))
                builder.put(varName, e.getValue());
        }
        String filterString = toSPARQLSyntax(boundExpr);
        return new SPARQLFilter(filterString, boundExpr, builder.build());
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SPARQLFilter)) return false;
        SPARQLFilter that = (SPARQLFilter) o;
        return expr.equals(that.expr) &&
                var2term.equals(that.var2term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, var2term);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("FILTER(").append(filter).append(")");
        boolean trivial = var2term.entrySet().stream().allMatch(e ->
                e.getValue().isVar() && e.getKey().equals(e.getValue().asVar().getName()));
        if (!trivial) {
            b.append("@{");
            for (Map.Entry<String, Term> e : var2term.entrySet())
                b.append(e.getKey()).append('=').append(e.getValue()).append(", ");
            b.setLength(b.length()-2);
            b.append(')');
        }
        return b.toString();
    }
}
