package br.ufsc.lapesd.riefederator.jena;

import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.query.modifiers.FilterParsingException;
import com.google.common.collect.Sets;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExprUtils {
    private static final Logger logger = LoggerFactory.getLogger(ExprUtils.class);

    public static final @Nonnull Set<String> BI_OPS = Sets.newHashSet("<", "<=", "=", "==", "!=",
                                                                      ">=", ">", "+", "-", "*", "/",
                                                                      "&&", "||");

    public static final @Nonnull Set<String> L_BI_OPS = Sets.newHashSet("&&", "||");
    public static final @Nonnull Set<String> UN_OPS = Sets.newHashSet("!", "-", "+");
    public static final @Nonnull Set<String> L_UN_OPS = Collections.singleton("!");
    public static final @Nonnull Set<String> L_OPS = Sets.newHashSet("&&", "||", "!");
    private static final @Nonnull Pattern varNameRx =
            Pattern.compile("(?:^|\\s|[,(.])[$?]([^ \t.),\n]+)(?:\\s|[,.)]|$)");
    private static final @Nonnull SerializationContext SER_CTX = new SerializationContext();


    public static @Nonnull String innerExpression(@Nonnull String string) {
        return string.replaceAll("(?i)^\\s*FILTER\\((.*)\\)\\s*$", "$1");
    }

    public static @Nonnull Expr parseFilterExpr(@Nonnull String string) {
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

    public static  @Nonnull StringBuilder toSPARQLSyntax(@Nonnull Expr expr,
                                                          @Nonnull StringBuilder builder) {
        if      (expr instanceof NodeValue) return builder.append(expr.toString());
        else if (expr instanceof ExprVar) return builder.append(expr.toString());
        else if (expr instanceof ExprNone) return builder;
        else if (expr instanceof ExprAggregator) {
            logger.warn("Did not expect aggregator {} within a FILTER()s!", expr);
            return builder;
        }

        assert expr instanceof ExprFunction;
        ExprFunction function = (ExprFunction) expr;
        String name = getFunctionName(function);

        if (function.numArgs() == 2 && BI_OPS.contains(name)) {
            toSPARQLSyntax(function.getArg(1), builder).append(' ').append(name).append(' ');
            return toSPARQLSyntax(function.getArg(2), builder);
        } else if (function.numArgs() == 1 && UN_OPS.contains(name)) {
            builder.append(name).append('(');
            return toSPARQLSyntax(function.getArg(1), builder).append(')');
        } else {
            builder.append(name).append('(');
            for (int i = 1; i <= function.numArgs(); i++)
                toSPARQLSyntax(function.getArg(i), builder).append(", ");
            if (function.numArgs() > 0)
                builder.setLength(builder.length()-2);
            return builder.append(')');
        }
    }

    public static @Nonnull String getFunctionName(@Nonnull ExprFunction function) {
        String name = function.getOpName();
        if (name == null)
            name = function.getFunctionName(SER_CTX);
        return name;
    }

    public static @Nonnull String toSPARQLSyntax(@Nonnull Expr expr) {
        return toSPARQLSyntax(expr, new StringBuilder()).toString();
    }
}
