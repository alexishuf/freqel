package br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl;

import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.PrimitiveParser;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PrimitiveParserParser {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(PrimitiveParserParser.class);

    @Contract("null -> null")
    public static @Nullable PrimitiveParser parse(@Nullable DictTree dict) {
        if (dict == null) return null;
        String type = dict.getString("parser", "");
        if (type.equals("date"))
            return DatePrimitiveParser.tryCreate(dict.getString("date-format", ""));
        logger.warn("Unknown parser type: {}", type);
        return null;
    }
}
