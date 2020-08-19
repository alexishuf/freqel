package br.ufsc.lapesd.riefederator.query.parse.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.jena.model.prefix.PrefixMappingDict;
import br.ufsc.lapesd.riefederator.jena.query.JenaBindingSolution;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryVisitor;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.path.*;
import org.apache.jena.sparql.syntax.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toSet;

public class ConvertVisitor implements QueryVisitor {
    private static final Logger logger = LoggerFactory.getLogger(ConvertVisitor.class);

    private final @Nonnull ConvertOptions convertOptions;
    private Op op;
    private ModifiersSet outerModifiers;
    private Set<SPARQLFilter> groupFilters;
    private PrefixDict prefixDict;
    private Set<String> allVars;
    private int lastHidden = -1;

    public ConvertVisitor(@Nonnull ConvertOptions convertOptions) {
        this.convertOptions = convertOptions;
    }

    public @Nonnull Op getTree() {
        return op;
    }

    public static class FeatureException extends RuntimeException {
        public FeatureException(@Nonnull String message) {
            super(message);
        }
    }
    @Override
    public void startVisit(Query query) {
        outerModifiers = new ModifiersSet();
        allVars = new HashSet<>();
    }
    @Override
    public void visitPrologue(Prologue prologue) {
        prefixDict = new PrefixMappingDict(prologue.getPrefixMapping());
    }
    @Override
    public void visitResultForm(Query query) { }
    @Override
    public void visitSelectResultForm(Query query) {
        if (query.isDistinct())
            outerModifiers.add(Distinct.INSTANCE);
        Set<String> vars = query.getProjectVars().stream().map(Var::getVarName).collect(toSet());
        outerModifiers.add(Projection.of(vars));
    }
    @Override
    public void visitConstructResultForm(Query query) {
        throw new FeatureException("CONSTRUCT queries are not supported");
    }
    @Override
    public void visitDescribeResultForm(Query query) {
        throw new FeatureException("DESCRIBE queries are not supported");
    }
    @Override
    public void visitAskResultForm(Query query) {
        outerModifiers.add(Ask.INSTANCE);
    }
    @Override
    public void visitJsonResultForm(Query query) { }
    @Override
    public void visitDatasetDecl(Query query) { }

    private void addTriple(@Nonnull MutableCQuery query,
                           @Nonnull Term s, @Nonnull Term p, @Nonnull Term o) {
        addTriple(query, new br.ufsc.lapesd.riefederator.model.Triple(s, p, o));
    }
    private void addTriple(@Nonnull MutableCQuery query,
                           @Nonnull br.ufsc.lapesd.riefederator.model.Triple triple) {
        triple.forEach(t -> {
            if (t.isVar())
                allVars.add(t.asVar().getName());
        });
        query.add(triple);
    }


    @Override
    public void visitQueryPattern(Query query) {
        query.getQueryPattern().visit(new ElementVisitor());
    }
    @Override
    public void visitGroupBy(Query query) {
        if (query.hasGroupBy() && !convertOptions.getEraseGroupBy())
            throw new FeatureException("GROUP BY is not supported");
    }
    @Override
    public void visitHaving(Query query) {
        if (query.hasHaving())
            throw new FeatureException("HAVING is not supported");
    }
    @Override
    public void visitOrderBy(Query query) {
        if (query.hasOrderBy() && !convertOptions.getEraseOrderBy())
            throw new FeatureException("ORDER BY is not supported");
    }
    @Override
    public void visitLimit(Query query) {
        if (query.hasLimit())
            outerModifiers.add(Limit.of((int)query.getLimit()));
    }
    @Override
    public void visitOffset(Query query) {
        if (query.hasOffset() && !convertOptions.getEraseOffset())
            throw new FeatureException("OFFSET is not supported");
    }
    @Override
    public void visitValues(Query query) {
        ImmutableSet.Builder<String> namesBuilder = ImmutableSet.builder();
        if (query.getValuesVariables() == null)
            return; // no work to do
        for (Var v : query.getValuesVariables())
            namesBuilder.add(v.getVarName());
        ImmutableSet<String> names = namesBuilder.build();

        ImmutableList.Builder<Solution> solutions = ImmutableList.builder();
        JenaBindingSolution.Factory factory = JenaBindingSolution.forVars(names);
        for (Binding binding : query.getValuesData())
            solutions.add(factory.apply(binding));
        outerModifiers.add(new ValuesModifier(names, solutions.build()));
    }
    @Override
    public void finishVisit(Query q) {
        assert op != null;
        Projection p = outerModifiers.projection();
        if (p != null && !q.isAskType()) {
            Set<String> pVars = p.getVarNames();
            if (pVars.equals(allVars)) {
                outerModifiers.remove(p); // no projection
            } else if (!convertOptions.getAllowExtraProjections() && !allVars.containsAll(pVars)) {
                throw new IllegalArgumentException("There projected variables that cannot " +
                                                   "be bound from any triple pattern");
            }
        }
        op.modifiers().addAll(outerModifiers);
        if (prefixDict != null) {
            TreeUtils.streamPreOrder(op).filter(QueryOp.class::isInstance)
                     .forEach(o -> ((QueryOp) o).getQuery().setPrefixDict(prefixDict));
        }
    }

    private class ElementVisitor extends ElementVisitorBase {
        private @Nonnull MutableCQuery getMutableCQuery() {
            if (op instanceof QueryOp) {
                return ((QueryOp)op).getQuery();
            } else if (op instanceof ConjunctionOp) {
                List<Op> list = op.getChildren();
                for (ListIterator<Op> it = list.listIterator(list.size()); it.hasPrevious(); ) {
                    Op child = it.previous();
                    if (child instanceof QueryOp)
                        return ((QueryOp) child).getQuery();
                }
            }
            return new MutableCQuery();
        }

        private void saveCQuery(@Nonnull MutableCQuery query) {
            if (op == null) {
                op = new QueryOp(query);
            } else if (op instanceof QueryOp) {
                assert query.containsAll(((QueryOp) op).getQuery())
                        : "Saving query into FreeQueryOp looses triples!";
                ((QueryOp) op).setQuery(query);
            } else if (op instanceof ConjunctionOp) {
                List<Op> list = op.getChildren();
                for (ListIterator<Op> it = list.listIterator(list.size()); it.hasPrevious(); ) {
                    Op previous = it.previous();
                    if (previous instanceof QueryOp) {
                        assert query.containsAll(((QueryOp) previous).getQuery())
                                : "Saving query into old child of ConjunctionOp looses triples";
                        ((QueryOp) previous).setQuery(query);
                        return;
                    }
                }
                ((ConjunctionOp) op).addChild(new QueryOp(query));
            } else {
                op = ConjunctionOp.builder().add(op).add(new QueryOp(query)).build();
            }
        }

        @Override
        public void visit(ElementTriplesBlock el) {
            MutableCQuery cQuery = getMutableCQuery();
            for (Iterator<Triple> it = el.patternElts(); it.hasNext(); )
                addTriple(cQuery, fromJena(it.next()));
            saveCQuery(cQuery);
        }

        @Override
        public void visit(ElementFilter el) {
            Expr expr = el.getExpr();
            if (expr instanceof E_Exists)
                throw new FeatureException("FILTER EXISTS is not supported");
            if (expr instanceof E_NotExists)
                throw new FeatureException("FILTER NOT EXISTS is not supported");
            groupFilters.add(SPARQLFilter.build(expr));
        }

        @Override
        public void visit(ElementAssign el) {
            throw new FeatureException("Variable assignments are not supported");
        }

        @Override
        public void visit(ElementBind el) {
            throw new FeatureException("BIND is not supported");
        }

        @Override
        public void visit(ElementData el) {
            throw new FeatureException("VALUES is not supported");
        }

        @Override
        public void visit(ElementUnion el) {
            Op oldOp = ConvertVisitor.this.op;
            List<Op> list = new ArrayList<>();
            for (Element element : el.getElements()) {
                op = null;
                element.visit(this);
                list.add(op);
            }
            if (oldOp instanceof UnionOp) {
                try (TakenChildren children = ((UnionOp) oldOp).takeChildren()) {
                    children.addAll(list);
                }
                op = oldOp;
            } else {
                Op unionOp = UnionOp.build(list);
                if (list.size() > 1)
                    unionOp.modifiers().add(Distinct.INSTANCE);
                if (oldOp instanceof ConjunctionOp) {
                    ((ConjunctionOp) oldOp).addChild(unionOp);
                    ConvertVisitor.this.op = oldOp;
                } else if (oldOp != null) {
                    op = ConjunctionOp.builder().add(oldOp).add(unionOp).build();
                } else {
                    op = unionOp;
                }
            }
        }

        @Override
        public void visit(ElementDataset el) {
            throw new FeatureException("datasets specifications are not supported");
        }

        @Override
        public void visit(ElementOptional el) {
            /* Filter elements can be safely ignored, since all optional filters are
               trivially true. A filter never binds a variable, so there is no use
               evaluating it (as a group pattern is) */
            if (!(el.getOptionalElement() instanceof ElementFilter)) {
                Op oldOp = ConvertVisitor.this.op;
                op = null;
                el.getOptionalElement().visit(this);
                if (op == null) {
                    logger.warn("Ignoring OPTIONAL that yields no Op: {}", el.getOptionalElement());
                    return;
                }
                op.modifiers().add(Optional.INSTANCE);
                if (oldOp instanceof ConjunctionOp) {
                    ((ConjunctionOp) oldOp).addChild(op);
                    op = oldOp;
                } else if (oldOp != null) {
                    op = ConjunctionOp.builder().add(oldOp).add(op).build();
                }
            }
        }

        @Override
        public void visit(ElementGroup el) {
            Set<SPARQLFilter> oldGroupFilters = groupFilters;
            groupFilters = new HashSet<>();
            el.getElements().forEach(e -> e.visit(this));
            assert op != null;
            op.modifiers().addAll(groupFilters);
            groupFilters = oldGroupFilters;
        }

        @Override
        public void visit(ElementNamedGraph el) {
            throw new FeatureException("Named graphs are not supported");
        }

        @Override
        public void visit(ElementExists el) {
            throw new FeatureException("FILTER EXISTS is not supported");
        }

        @Override
        public void visit(ElementNotExists el) {
            throw new FeatureException("FILTER NOT EXISTS is not supported");
        }

        @Override
        public void visit(ElementMinus el) {
            throw new FeatureException("MINUS is not supported");
        }

        @Override
        public void visit(ElementService el) {
            throw new FeatureException("SERVICE is not supported");
        }

        @Override
        public void visit(ElementSubQuery el) {
            throw new FeatureException("Subqueries are not supported");
        }

        @Override
        public void visit(ElementPathBlock el) {
            MutableCQuery cQuery = getMutableCQuery();
            for (Iterator<TriplePath> it = el.patternElts(); it.hasNext(); ) {
                TriplePath path = it.next();
                Triple triple = path.asTriple();
                if (triple != null) {
                    addTriple(cQuery, fromJena(triple));
                    continue;
                }
                List<Term> terms = new ArrayList<>();
                terms.add(fromJena(path.getSubject()));
                path.getPath().visit(new PathVisitor() {
                    @Override
                    public void visit(P_Link pathNode) {
                        terms.add(fromJena(pathNode.getNode()));
                    }

                    @Override
                    public void visit(P_Seq pathSeq) {
                        pathSeq.getLeft().visit(this);
                        StdVar hidden = SPARQLParser.hidden(++lastHidden);
                        terms.add(hidden); //as object ...
                        terms.add(hidden); //and subject of next triple
                        pathSeq.getRight().visit(this);
                    }

                    @Override
                    public void visit(P_ReverseLink pathNode) {
                        throw new FeatureException("SPARQL 1.1 paths with ^ are not supported (yet)");
                    }

                    @Override
                    public void visit(P_NegPropSet pathNotOneOf) {
                        throw new FeatureException("SPARQL 1.1 paths with ! are not supported");
                    }

                    @Override
                    public void visit(P_Inverse inversePath) {
                        throw new FeatureException("SPARQL 1.1 paths with ^ are not supported (yet)");
                    }

                    @Override
                    public void visit(P_Mod pathMod) {
                        throw new FeatureException("Jena {M,N} path extension is not supported");
                    }

                    @Override
                    public void visit(P_FixedLength pFixedLength) {
                        throw new FeatureException("Jena {N} path extension is not supported");
                    }

                    @Override
                    public void visit(P_Distinct pathDistinct) {
                        throw new FeatureException("Jena P_Distinct path extension is not supported");
                    }

                    @Override
                    public void visit(P_Multi pathMulti) {
                        throw new FeatureException("Jena P_Multi path extension is not supported");
                    }

                    @Override
                    public void visit(P_Shortest pathShortest) {
                        throw new FeatureException("Jena shortest path extension is not supported");
                    }

                    @Override
                    public void visit(P_ZeroOrOne path) {
                        throw new FeatureException("SPARQL 1.1 paths with ? are not supported");
                    }

                    @Override
                    public void visit(P_ZeroOrMore1 path) {
                        throw new FeatureException("SPARQL 1.1 paths with * are not supported");
                    }

                    @Override
                    public void visit(P_ZeroOrMoreN path) {
                        throw new FeatureException("SPARQL 1.1 paths with * are not supported");
                    }

                    @Override
                    public void visit(P_OneOrMore1 path) {
                        throw new FeatureException("SPARQL 1.1 paths with + are not supported");
                    }

                    @Override
                    public void visit(P_OneOrMoreN path) {
                        throw new FeatureException("SPARQL 1.1 paths with + are not supported");
                    }

                    @Override
                    public void visit(P_Alt pathAlt) {
                        throw new FeatureException("SPARQL 1.1 paths with | are not supported");
                    }
                });
                terms.add(fromJena(path.getObject()));
                checkArgument((terms.size() % 3) == 0, "SPARQL 1.1 path yielded " +
                              "a triple with less than 3 terms. This is likely a bug");
                for (int i = 0; i < terms.size(); i += 3)
                    addTriple(cQuery, terms.get(i), terms.get(i+1), terms.get(i+2));
            }
            saveCQuery(cQuery);
        }
    }
}
