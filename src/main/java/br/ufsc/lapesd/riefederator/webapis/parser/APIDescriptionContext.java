package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.util.MediaTypePredicate;
import br.ufsc.lapesd.riefederator.util.PatternMap;
import br.ufsc.lapesd.riefederator.util.PatternPredicate;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.ResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.TermSerializer;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimitsRegistry;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.util.regex.Pattern;

public class APIDescriptionContext {
    private @Nonnull PatternMap<PagingStrategy> pagingStrategies = new PatternMap<>();
    private @Nonnull PatternMap<TermSerializer> termSerializers = new PatternMap<>();
    private @Nonnull PatternMap<RateLimitsRegistry> rateLimitsRegistries = new PatternMap<>();
    private @Nonnull PatternMap<ResponseParser> responseParsers = new PatternMap<>();

    /* --- --- --- --- PagingStrategy --- --- --- --- */

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setPagingStrategy(@Nonnull Pattern endpointRx,
                                                            @Nonnull PagingStrategy strategy) {
        pagingStrategies.add(new PatternPredicate(endpointRx), PatternMap.ANY, strategy);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setPagingStrategy(@Nonnull PagingStrategy strategy) {
        pagingStrategies.add(PatternMap.ANY, PatternMap.ANY, strategy);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetPagingStrategy(@Nullable Pattern endpointRx) {
        pagingStrategies.remove(endpointRx == null ? null : new PatternPredicate(endpointRx),
                                null);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetPagingStrategy() {
        return unsetPagingStrategy(null);
    }

    @CheckReturnValue
    public @Nullable PagingStrategy getPagingStrategy(@Nonnull String endpoint) {
        return pagingStrategies.getLast(endpoint, "");
    }

    /* --- --- --- --- TermSerializer --- --- --- --- */

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setSerializer(@Nonnull Pattern endpointRx,
                                                        @Nonnull Pattern inputRx,
                                                        @Nonnull TermSerializer serializer) {
        termSerializers.add(new PatternPredicate(endpointRx), new PatternPredicate(inputRx),
                            serializer);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setSerializer(@Nonnull String endpointRx,
                                                        @Nonnull String inputRx,
                                                        @Nonnull TermSerializer serializer) {
        termSerializers.add(new PatternPredicate(endpointRx), new PatternPredicate(inputRx),
                serializer);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setSerializer(@Nonnull Pattern inputRx,
                                                        @Nonnull TermSerializer serializer) {
        termSerializers.add(PatternMap.ANY, new PatternPredicate(inputRx), serializer);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setSerializer(@Nonnull String inputRx,
                                                        @Nonnull TermSerializer serializer) {
        termSerializers.add(PatternMap.ANY, new PatternPredicate(inputRx), serializer);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetSerializer(@Nullable Pattern endpointRx,
                                                          @Nullable Pattern inputRx) {
        PatternPredicate p1 = endpointRx == null ? null : new PatternPredicate(endpointRx);
        PatternPredicate p2 = inputRx == null ? null : new PatternPredicate(inputRx);
        termSerializers.remove(p1, p2);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetSerializer(@Nullable String endpointRx,
                                                          @Nullable String inputRx) {
        PatternPredicate p1 = endpointRx == null ? null : new PatternPredicate(endpointRx);
        PatternPredicate p2 = inputRx == null ? null : new PatternPredicate(inputRx);
        termSerializers.remove(p1, p2);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetSerializer() {
        return unsetSerializer((Pattern) null, null);
    }

    @CheckReturnValue
    public @Nullable TermSerializer getSerializer(@Nonnull String endpoint, @Nonnull String input){
        return termSerializers.getLast(endpoint, input);
    }

    /* --- --- --- --- RateLimitsRegistry --- --- --- --- */

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setRateLimitRegistry(@Nonnull Pattern endpointRx,
                                                               @Nonnull RateLimitsRegistry reg) {
        rateLimitsRegistries.add(new PatternPredicate(endpointRx), PatternMap.ANY, reg);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setRateLimitRegistry(@Nonnull String endpointRx,
                                                               @Nonnull RateLimitsRegistry reg) {
        rateLimitsRegistries.add(new PatternPredicate(endpointRx), PatternMap.ANY, reg);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setRateLimitRegistry(@Nonnull RateLimitsRegistry reg) {
        rateLimitsRegistries.add(PatternMap.ANY, PatternMap.ANY, reg);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetRateLimitRegistry(@Nullable Pattern endpointRx) {
        PatternPredicate p = endpointRx == null ? null : new PatternPredicate(endpointRx);
        rateLimitsRegistries.remove(p, null);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetRateLimitRegistry(@Nullable String endpointRx) {
        PatternPredicate p = endpointRx == null ? null : new PatternPredicate(endpointRx);
        rateLimitsRegistries.remove(p, null);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetRateLimitRegistry() {
        return unsetRateLimitRegistry((Pattern) null);
    }

    @CheckReturnValue
    public @Nullable RateLimitsRegistry getRateLimitRegistry(@Nonnull String endpoint) {
        return rateLimitsRegistries.getLast(endpoint, "");
    }

    /* --- --- --- --- ResponseParser --- --- --- --- */

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setResponseParser(@Nonnull MediaType acceptMediaType,
                                                            @Nonnull ResponseParser responseParser){
        responseParsers.add(PatternMap.ANY, new MediaTypePredicate(acceptMediaType), responseParser);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setResponseParser(@Nonnull Pattern endpointRx,
                                                            @Nonnull MediaType acceptMediaType,
                                                            @Nonnull ResponseParser responseParser){
        responseParsers.add(new PatternPredicate(endpointRx),
                            new MediaTypePredicate(acceptMediaType), responseParser);
        return this;
    }


    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext setResponseParser(@Nonnull String endpointRx,
                                                            @Nonnull MediaType acceptMediaType,
                                                            @Nonnull ResponseParser responseParser){
        responseParsers.add(new PatternPredicate(endpointRx),
                new MediaTypePredicate(acceptMediaType), responseParser);
        return this;
    }
    
    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetResponseParser(@Nullable Pattern endpointRx, 
                                                              @Nullable MediaType acceptMediaType) {
        PatternPredicate p1 = endpointRx == null ? null : new PatternPredicate(endpointRx);
        MediaTypePredicate p2 = acceptMediaType == null 
                ? null : new MediaTypePredicate(acceptMediaType);
        responseParsers.remove(p1, p2);
        return this;        
    }

    @CanIgnoreReturnValue
    public @Nonnull APIDescriptionContext unsetResponseParser(@Nullable String endpointRx,
                                                              @Nullable MediaType acceptMediaType) {
        PatternPredicate p1 = endpointRx == null ? null : new PatternPredicate(endpointRx);
        MediaTypePredicate p2 = acceptMediaType == null
                ? null : new MediaTypePredicate(acceptMediaType);
        responseParsers.remove(p1, p2);
        return this;
    }


    @CheckReturnValue
    public @Nullable ResponseParser getResponseParser(@Nonnull String endpoint,
                                                      @Nonnull MediaType mediaType) {
        return getResponseParser(endpoint, mediaType.toString());
    }
    @CheckReturnValue
    public @Nullable ResponseParser getResponseParser(@Nonnull String endpoint,
                                                      @Nonnull String mediaType) {
        return responseParsers.getLast(endpoint, mediaType);
    }
}
