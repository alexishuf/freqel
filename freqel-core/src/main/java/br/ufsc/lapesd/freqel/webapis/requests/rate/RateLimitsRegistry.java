package br.ufsc.lapesd.freqel.webapis.requests.rate;

import br.ufsc.lapesd.freqel.webapis.requests.rate.impl.NoRateLimit;
import br.ufsc.lapesd.freqel.webapis.requests.rate.impl.SimpleRateLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RateLimitsRegistry {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(RateLimitsRegistry.class);
    private static final Pattern HOST_RX = Pattern.compile("^\\s*(\\w+://)?([^@]*@)?([^/]*)");
    public static final @Nonnull RateLimitsRegistry INSTANCE = new RateLimitsRegistry();

    private final @Nonnull LinkedHashMap<String, RateLimit> prefix2Limit = new LinkedHashMap<>();

    public static @Nonnull RateLimitsRegistry getGlobal() {
        return INSTANCE;
    }

    public synchronized  @Nullable RateLimit register(@Nonnull String prefix,
                                                      @Nullable RateLimit limit) {
        Matcher matcher = HOST_RX.matcher(prefix);
        if (!matcher.find()) {
            throw new IllegalArgumentException("prefix "+prefix+" does not look like an " +
                                               "URI nor hostname!");
        }
        String host = matcher.group(3);
        if (limit == null)
            return prefix2Limit.remove(host);
        else
            return prefix2Limit.put(host, limit);
    }

    public synchronized  @Nullable RateLimit unregister(@Nonnull String prefix) {
        return register(prefix, null);
    }

    private synchronized  @Nonnull RateLimit getForHost(@Nonnull String host) {
        RateLimit limit = prefix2Limit.getOrDefault(host, null);
        if (limit == null && this != INSTANCE)
            return INSTANCE.getForHost(host); //fallback to global registry
        if (limit == null) {
            // default: disallow parallel requests, but do not enforce sleep
            INSTANCE.register(host, limit = new SimpleRateLimit(0));
        }
        return limit;
    }

    public synchronized  @Nonnull RateLimit get(@Nonnull String uri) {
        Matcher matcher = HOST_RX.matcher(uri);
        if (!matcher.find())
            logger.warn("Failed to extract host of {}. Will return NoRateLimit", uri);
        return getForHost(matcher.group(3));
    }

}
