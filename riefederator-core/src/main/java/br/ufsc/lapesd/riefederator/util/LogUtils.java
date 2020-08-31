package br.ufsc.lapesd.riefederator.util;

import br.ufsc.lapesd.riefederator.model.SPARQLString;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import org.apache.jena.query.Query;
import org.apache.jena.query.Syntax;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogUtils {
    private static final Pattern PREFIX_RX = Pattern.compile("^ *PREFIX *([^:]*):.*$");

    private static void logQuery(@Nonnull Logger logger, @Nonnull String query,
                                @Nonnull TPEndpoint ep, int results, @Nullable Stopwatch sw) {
        if (!logger.isDebugEnabled()) return;
        if (results >= 0) {
            if (sw != null) {
                logger.trace("Got {} results from {} in {}ms for \"\"\"{}\"\"\"",
                             results, ep, sw.elapsed(TimeUnit.MICROSECONDS)/1000.0, query);
            } else {
                logger.trace("Got {} results from {} for \"\"\"{}\"\"\"",
                        results, ep, query);
            }
        } else {
            if (sw != null) {
                logger.trace("Sent query to {} in {}ms. Query:\"\"\"{}\"\"\"",
                             ep, sw.elapsed(TimeUnit.MICROSECONDS)/1000.0, query);
            } else {
                logger.trace("Sent query to {}: \"\"\"{}\"\"\"", ep, query);
            }
        }
    }

    public static void logQuery(@Nonnull Logger logger, @Nonnull CQuery query,
                                @Nonnull TPEndpoint ep, int results, @Nullable Stopwatch sw) {
        logQuery(logger, toString(query), ep, results, sw);
    }
    public static void logQuery(@Nonnull Logger logger, @Nonnull SPARQLString query,
                                @Nonnull TPEndpoint ep, int results, @Nullable Stopwatch sw) {
        logQuery(logger, toString(query), ep, results, sw);
    }
    public static void logQuery(@Nonnull Logger logger, @Nonnull Query query,
                                @Nonnull TPEndpoint ep, int results, @Nullable Stopwatch sw) {
        logQuery(logger, toString(query), ep, results, sw);
    }
    public static void logQuery(@Nonnull Logger logger, @Nonnull CQuery query,
                                @Nonnull TPEndpoint ep, @Nullable Stopwatch sw) {
        logQuery(logger, toString(query), ep, -1, sw);
    }
    public static void logQuery(@Nonnull Logger logger, @Nonnull SPARQLString query,
                                @Nonnull TPEndpoint ep, @Nullable Stopwatch sw) {
        logQuery(logger, toString(query), ep, -1, sw);
    }
    public static void logQuery(@Nonnull Logger logger, @Nonnull Query query,
                                @Nonnull TPEndpoint ep, @Nullable Stopwatch sw) {
        logQuery(logger, toString(query), ep, -1, sw);
    }

    public static @Nonnull String toString(CQuery query) {
        return toString(SPARQLString.create(query.withPrefixDict(StdPrefixDict.STANDARD)));
    }
    public static @Nonnull String toString(Query query) {
        return query.toString(Syntax.syntaxSPARQL_11);
    }
    public static @Nonnull String toString(SPARQLString query) {
        PrefixDict d = StdPrefixDict.DEFAULT;
        boolean small = query.getSparql().split("\n").length <= 4;
        StringBuilder builder = new StringBuilder();
        for (String line : Splitter.on('\n').omitEmptyStrings().splitToList(query.toString())) {
            Matcher matcher = PREFIX_RX.matcher(line);
            if (!matcher.matches() || d.expandPrefix(matcher.group(1), null) == null) {
                builder.append(line).append(small ? ' ' : '\n');
            }
        }
        builder.setLength(builder.length()-1);
        return builder.toString();
    }
}
