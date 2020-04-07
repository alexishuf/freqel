package br.ufsc.lapesd.riefederator.query.parse;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.jena.model.prefix.PrefixMappingDict;
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
import org.apache.jena.sparql.syntax.*;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

public class SPARQLQueryParser {
    public static @Nonnull CQuery parse(@Nonnull String sparql) throws SPARQLParseException {
        try {
            return convert(QueryFactory.create(sparql));
        } catch (QueryParseException e) {
            throw new SPARQLParseException("SPARQL syntax error", e, sparql);
        }
    }

    public static @Nonnull CQuery parse(@Nonnull Reader sparqlReader) throws SPARQLParseException {
        try {
            return parse(IOUtils.toString(sparqlReader));
        } catch (IOException e) {
            throw new SPARQLParseException("Failed to read SPARQL from reader", e, null);
        }
    }


    private static class FeatureException extends RuntimeException {
        final @Nonnull String message;
        public FeatureException(@Nonnull String message) {
            super(message);
            this.message = message;
        }
    }

    public static  @Nonnull CQuery convert(@Nonnull Query q) throws SPARQLParseException {
        CQuery.Builder builder = CQuery.builder();
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
                    // SELECT * will cause all vars to appear here. We add them as projection and
                    // CQuery.WithBuilder.build() will forgo the modifier if it detects a
                    // projection of all variables
                    query.getProjectVars().stream().map(Var::getVarName).forEach(builder::project);
                    builder.requireProjection();
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
                @Override
                public void visitQueryPattern(Query query) {
                    query.getQueryPattern().visit(new ElementVisitorBase() {
                        @Override
                        public void visit(ElementTriplesBlock el) {
                            el.patternElts().forEachRemaining(t
                                    -> builder.add(JenaWrappers.fromJena(t)));
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
                                if (triple != null)
                                    builder.add(JenaWrappers.fromJena(triple));
                                else
                                    throw new FeatureException("SPARQL 1.1 paths are not supported");
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
                        throw new FeatureException("LIMIT is not supported");
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
        } catch (FeatureException e) {
            throw new UnsupportedSPARQLFeatureException(e.message, q);
        }
        return builder.build();
    }

    public static void main(String[] args) {
        Query select = QueryFactory.create("PREFIX xsd: <" + XSD.NS + ">\n" +
                "SELECT DISTINCT * WHERE {\n" +
                "  ?x a xsd:int.\n" +
                "  ?x ?p ?o.\n" +
                "}");
        Query ask = QueryFactory.create("PREFIX xsd: <" + XSD.NS + ">\n" +
                "ASK WHERE {\n" +
                "  ?x a xsd:int.\n" +
                "}");
        Query path = QueryFactory.create("PREFIX xsd: <" + XSD.NS + ">\n" +
                "PREFIX foaf: <"+ FOAF.NS+">\n" +
                "PREFIX  rdf: <"+ RDF.getURI() +">\n" +
                "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                "SELECT ?x WHERE {\n" +
                "  ?x rdf:type/rdfs:subClassOf* foaf:Agent.\n" +
                "}");
        System.out.println("HELLO");
    }
}
