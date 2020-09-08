package br.ufsc.lapesd.riefederator.jena;

import com.google.common.collect.Sets;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class ExprUtils {
    private static final Logger logger = LoggerFactory.getLogger(ExprUtils.class);

    public static final @Nonnull Set<String> BI_OPS = Sets.newHashSet("<", "<=", "=", "==", "!=",
                                                                      ">=", ">", "+", "-", "*", "/",
                                                                      "&&", "||");

    public static final @Nonnull Set<String> L_BI_OPS = Sets.newHashSet("&&", "||");
    public static final @Nonnull Set<String> UN_OPS = Sets.newHashSet("!", "-", "+");
    public static final @Nonnull Set<String> L_UN_OPS = Collections.singleton("!");
    public static final @Nonnull Set<String> L_OPS = Sets.newHashSet("&&", "||", "!");

    private static final @Nonnull SerializationContext SER_CTX = new SerializationContext();


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
