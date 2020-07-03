package br.ufsc.lapesd.riefederator.query.parse;

import br.ufsc.lapesd.riefederator.jena.model.prefix.PrefixMappingDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.QueryVisitor;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.path.*;
import org.apache.jena.sparql.syntax.*;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static com.google.common.base.Preconditions.checkArgument;

public class SPARQLQueryParser {
    private final static String HIDDEN_VAR_PREFIX = "parserPathHiddenVar";
    private final static SPARQLQueryParser INSTANCE = new SPARQLQueryParser();
    private final static SPARQLQueryParser TOLERANT = new SPARQLQueryParser()
            .allowExtraProjections(true);
    private boolean allowExtraProjections = false;

    static @Nonnull StdVar hidden(int id) {
        return new StdVar(HIDDEN_VAR_PREFIX+id);
    }
    static boolean isHidden(@Nonnull Term term) {
        return term.isVar() && term.asVar().getName().matches(HIDDEN_VAR_PREFIX+"\\d+");
    }

    public @Nonnull SPARQLQueryParser allowExtraProjections(boolean value) {
        allowExtraProjections = value;
         return this;
    }

    public static @Nonnull SPARQLQueryParser tolerant() {
        return TOLERANT;
    }

    public static @Nonnull SPARQLQueryParser strict() {
        return INSTANCE;
    }

    /* ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- v*/

    public @Nonnull CQuery parse(@Nonnull String sparql) throws SPARQLParseException {
        try {
            return convert(QueryFactory.create(sparql));
        } catch (QueryParseException e) {
            throw new SPARQLParseException("SPARQL syntax error: "+e.getMessage(), e, sparql);
        }
    }

    public @Nonnull CQuery parse(@Nonnull Reader sparqlReader) throws SPARQLParseException {
        try {
            return parse(IOUtils.toString(sparqlReader));
        } catch (IOException e) {
            throw new SPARQLParseException("Failed to read SPARQL from reader", e, null);
        }
    }

    public @Nonnull CQuery parse(@Nonnull InputStream stream) throws SPARQLParseException {
        return parse(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }

    public @Nonnull CQuery parse(@Nonnull File file) throws SPARQLParseException, IOException {
        try (FileInputStream stream = new FileInputStream(file)) {
            return parse(stream);
        }
    }

    private static class FeatureException extends RuntimeException {
        final @Nonnull String message;
        public FeatureException(@Nonnull String message) {
            super(message);
            this.message = message;
        }
    }

    public @Nonnull CQuery convert(@Nonnull Query q) throws SPARQLParseException {
        CQuery.Builder builder = CQuery.builder().allowExtraProjection(allowExtraProjections);
        Set<String> allResultVars = new HashSet<>();
        Set<String> projection = new HashSet<>();
        int[] lastHidden = {-1};
        try {
            q.visit(new QueryVisitor() {
                @Override
                public void startVisit(Query query) { }
                @Override
                public void visitPrologue(Prologue prologue) {
                    builder.prefixDict(new PrefixMappingDict(prologue.getPrefixMapping()));
                }
                @Override
                public void visitResultForm(Query query) {}
                @Override
                public void visitSelectResultForm(Query query) {
                    if (query.isDistinct())
                        builder.distinct(true);
                    query.getProjectVars().stream().map(Var::getVarName).forEach(projection::add);
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
                    builder.ask(false);
                }
                @Override
                public void visitJsonResultForm(Query query) {}
                @Override
                public void visitDatasetDecl(Query query) { }

                private void addTriple(@Nonnull Triple triple) {
                    addTriple(fromJena(triple));
                }
                private void addTriple(@Nonnull Term s, @Nonnull Term p, @Nonnull Term o) {
                    addTriple(new br.ufsc.lapesd.riefederator.model.Triple(s, p, o));
                }
                private void addTriple(@Nonnull br.ufsc.lapesd.riefederator.model.Triple triple) {
                    triple.forEach(t -> {
                        if (t.isVar())
                            allResultVars.add(t.asVar().getName());
                    });
                    builder.add(triple);
                }

                @Override
                public void visitQueryPattern(Query query) {
                    query.getQueryPattern().visit(new ElementVisitorBase() {
                        @Override
                        public void visit(ElementTriplesBlock el) {
                            el.patternElts().forEachRemaining(t
                                    -> builder.add(fromJena(t)));
                        }

                        @Override
                        public void visit(ElementFilter el) {
                            Expr expr = el.getExpr();
                            if (expr instanceof E_Exists)
                                throw new FeatureException("FILTER EXISTS is not supported");
                            if (expr instanceof E_NotExists)
                                throw new FeatureException("FILTER NOT EXISTS is not supported");
                            builder.modifier(SPARQLFilter.build(expr));
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
                            throw new FeatureException("UNION is not supported");
                        }

                        @Override
                        public void visit(ElementDataset el) {
                            throw new FeatureException("datasets specifications are not supported");
                        }

                        @Override
                        public void visit(ElementOptional el) {
                            throw new FeatureException("OPTIONAL is not supported");
                        }

                        @Override
                        public void visit(ElementGroup el) {
                            el.getElements().forEach(e -> e.visit(this));
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
                            for (Iterator<TriplePath> it = el.patternElts(); it.hasNext(); ) {
                                TriplePath path = it.next();
                                Triple triple = path.asTriple();
                                if (triple != null) {
                                    addTriple(triple);
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
                                        StdVar hidden = hidden(++lastHidden[0]);
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
                                    addTriple(terms.get(i), terms.get(i+1), terms.get(i+2));
                            }
                        }
                    });
                }
                @Override
                public void visitGroupBy(Query query) {
                    if (query.hasGroupBy())
                        throw new FeatureException("GROUP BY is not supported");
                }
                @Override
                public void visitHaving(Query query) {
                    if (query.hasHaving())
                        throw new FeatureException("HAVING is not supported");
                }
                @Override
                public void visitOrderBy(Query query) {
                    if (query.hasOrderBy())
                        throw new FeatureException("ORDER BY is not supported");
                }
                @Override
                public void visitLimit(Query query) {
                    if (query.hasLimit())
                        builder.limit(query.getLimit());
                }
                @Override
                public void visitOffset(Query query) {
                    if (query.hasOffset())
                        throw new FeatureException("OFFSET is not supported");
                }
                @Override
                public void visitValues(Query query) {
                    if (query.hasValues())
                        throw new FeatureException("VALUES is not supported");
                }
                @Override
                public void finishVisit(Query query) {}
            });
            if (!q.isAskType() && !projection.containsAll(allResultVars)) {
                projection.forEach(builder::project);
                builder.requireProjection();
            }
            return builder.build();
        } catch (FeatureException e) {
            throw new UnsupportedSPARQLFeatureException(e.message, q);
        } catch (RuntimeException e) {
            throw new SPARQLParseException("RuntimeException while parsing query: "+e.getMessage(),
                                           e, q.serialize());
        }
    }
}
