package br.ufsc.lapesd.riefederator.webapis.requests.paging.impl;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

@Immutable
public class ParamPagingStrategy implements PagingStrategy {
    private static final StdURI XSD_INT = new StdURI("http://www.w3.org/2001/XMLSchema#int");

    public static class ParamPager implements PagingStrategy.Pager, PagingStrategy.Pager.State {
        private @Nonnull String param;
        private int page;
        private final int increment;
        private boolean end;
        private final boolean endOnError, endOnNull, endOnEmptyResponse, endOnEmptyJson;
        private final @Nonnull ImmutableMap<String, Object> jsonEndValues;

        public ParamPager(@Nonnull String param, int firstPage, int increment,
                          boolean endOnError, boolean endOnNull, boolean endOnEmptyResponse,
                          boolean endOnEmptyJson,
                          @Nonnull ImmutableMap<String, Object> jsonEndValues) {
            this.param = param;
            this.page = firstPage;
            this.increment = increment;
            this.end = false;
            this.endOnError = endOnError;
            this.endOnNull = endOnNull;
            this.endOnEmptyResponse = endOnEmptyResponse;
            this.endOnEmptyJson = endOnEmptyJson;
            this.jsonEndValues = jsonEndValues;
        }

        @Override public @Nonnull State getPagerState() {
            return this;
        }

        @Override public void setPagerState(@Nonnull PagingStrategy.Pager.State state) {
            if (!(state instanceof ParamPager))
                throw new IllegalArgumentException();
            this.param = ((ParamPager)state).param;
            this.page = ((ParamPager)state).page;
            this.end = ((ParamPager)state).end;
            assert checkFinalFields((ParamPager) state);
        }

        private boolean checkFinalFields(@Nonnull ParamPager state) {
            assert this.increment == state.increment;
            assert this.endOnError == state.endOnError;
            assert this.endOnNull == state.endOnNull;
            assert this.endOnEmptyResponse == state.endOnEmptyResponse;
            assert this.endOnEmptyJson == state.endOnEmptyJson;
            assert this.jsonEndValues == state.jsonEndValues;
            return true;
        }

        @Override
        public boolean atEnd() {
            return end;
        }

        @Override
        public @Nonnull Solution apply(@Nonnull Solution solution) {
            return MapSolution.builder(solution)
                    .put(param, StdLit.fromUnescaped(String.valueOf(page), XSD_INT))
                    .build();
        }

        @Override
        public void notifyResponse(@Nonnull Response response) {
            if (end) return;
            if (endOnEmptyResponse && response.getLength() == 0) {
                end = true;
                return;
            }
            if (endOnError && response.getStatusInfo().getFamily() != SUCCESSFUL) {
                end = true;
                return;
            }
            boolean checkJson = endOnEmptyJson || !jsonEndValues.isEmpty();
            if (checkJson && response.getMediaType().isCompatible(APPLICATION_JSON_TYPE)) {
                response.bufferEntity();
                String json = response.readEntity(String.class);
                if (endOnEmptyJson && json.trim().isEmpty()) {
                    end = true;
                } else {
                    JsonElement e = new JsonParser().parse(json);
                    if (checkEmptyJson(e))
                        return;
                    for (Map.Entry<String, Object> entry : jsonEndValues.entrySet()) {
                        if (checkJsonValue(e, entry.getKey(), entry.getValue()))
                            return;
                    }
                }
            }
        }

        private boolean checkJsonValue(@Nonnull JsonElement root, @Nonnull String path,
                                       @Nullable Object value) {
            if (!root.isJsonObject()) return false;
            List<String> list = Splitter.on('/').omitEmptyStrings().trimResults().splitToList(path);
            JsonElement element = root;
            for (String property : list) {
                if (!element.isJsonObject())
                    return false; // path is broken
                element = element.getAsJsonObject().get(property);
            }
            if (valueMatches(element, value)) {
                end = true;
                return true;
            }
            return false;
        }

        private boolean valueMatches(@Nullable JsonElement element, @Nullable Object value) {
            if (value == null) {
                return element == null;
            } else {
                if (element == null) {
                    return false;
                } else if (element.isJsonObject()) {
                    return false;
                } else if (element.isJsonArray()) {
                    JsonArray asArray = element.getAsJsonArray();
                    if (value instanceof Object[]) {
                        Object[] a = (Object[]) value;
                        if (a.length == asArray.size()) {
                            for (int i = 0; i < a.length; i++) {
                                if (!valueMatches(asArray.get(i), a[i]))
                                    return false;
                            }
                            return true;
                        }
                    } else if (asArray.size() == 1) {
                        return valueMatches(asArray.get(0), value);
                    }
                } else if (value instanceof Integer || value instanceof Long) {
                    return element.getAsLong() == ((Number) value).longValue();
                } else if (value instanceof Float || value instanceof Double) {
                    return element.getAsDouble() == ((Number) value).doubleValue();
                } else {
                    return element.getAsString().equals(value.toString());
                }
            }
            return false;
        }

        private boolean checkEmptyJson(@Nonnull JsonElement e) {
            if (e.isJsonObject() && e.getAsJsonObject().size() == 0) {
                end = true;
                return true;
            }
            if (e.isJsonArray()) {
                JsonArray array = e.getAsJsonArray();
                if (array.size() == 0) {
                    end = true;
                    return true;
                } else {
                    boolean allEmpty = true;
                    for (JsonElement c : array) {
                        if (c.isJsonArray() ) {
                            if (!(allEmpty = c.getAsJsonArray().size() == 0)) break;
                        } else if (c.isJsonObject()) {
                            if (!(allEmpty = c.getAsJsonObject().size() == 0)) break;
                        } else {
                            allEmpty = false;
                            break;
                        }
                    }
                    if (!allEmpty) {
                        end = true;
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public void notifyResponseEndpoint(@Nullable CQEndpoint endpoint) {
            if (end) return;
            if ((endOnNull || endOnEmptyResponse) && endpoint == null) {
                end = true;
            } else if (endOnEmptyResponse && endpoint instanceof EmptyEndpoint) {
                end = true;
            } else if (endOnEmptyResponse && endpoint instanceof ARQEndpoint) {
                ARQEndpoint arqEndpoint = (ARQEndpoint) endpoint;
                if (arqEndpoint.isLocal() && arqEndpoint.isEmpty())
                    end = true;
            }
            if (!end)
                page += increment;
        }
    }

    private final @Nonnull String param;
    private final int firstPage, increment;
    private final boolean endOnError;
    private final boolean endOnEmpty;
    private final boolean endOnNull;
    private final boolean endOnEmptyJson;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableMap<String, Object> jsonEndValues;

    protected ParamPagingStrategy(@Nonnull String param, int firstPage, int increment,
                                  boolean endOnError, boolean endOnNull, boolean endOnEmpty,
                                  boolean endOnEmptyJson,
                                  @Nonnull ImmutableMap<String, Object> jsonEndValues) {
        this.param = param;
        this.firstPage = firstPage;
        this.increment = increment;
        this.endOnError = endOnError;
        this.endOnNull = endOnNull;
        this.endOnEmpty = endOnEmpty;
        this.endOnEmptyJson = endOnEmptyJson;
        this.jsonEndValues = jsonEndValues;
    }

    public static class Builder {
        private @Nonnull String param = "page";
        private int firstPage = 1, increment = 1;
        private boolean endOnError = true;
        private boolean endOnNull = true;
        private boolean endOnEmpty = true;
        private boolean endOnEmptyJson = false;
        private Map<String, Object> jsonEndValues = new HashMap<>();

        public @Nonnull Builder withParam(@Nonnull String param) {
            this.param = param;
            return this;
        }

        public @Nonnull Builder withFirstPage(int firstPage) {
            this.firstPage = firstPage;
            return this;
        }

        public @Nonnull Builder withIncrement(int increment) {
            this.increment = increment;
            return this;
        }

        public @Nonnull Builder withEndOnError(boolean endOnError) {
            this.endOnError = endOnError;
            return this;
        }

        public @Nonnull Builder withEndOnNull(boolean endOnNull) {
            this.endOnNull = endOnNull;
            return this;
        }

        public @Nonnull Builder withEndOnEmpty(boolean endOnEmpty) {
            this.endOnEmpty = endOnEmpty;
            return this;
        }

        public @Nonnull Builder withEndOnJsonValue(@Nonnull String path, @Nullable Object value) {
            this.jsonEndValues.put(path, value);
            return this;
        }

        public @Nonnull Builder withEndOnEmptyJson(boolean endOnEmptyJson) {
            this.endOnEmptyJson = endOnEmptyJson;
            return this;
        }

        public @Nonnull ParamPagingStrategy build() {
            return new ParamPagingStrategy(param, firstPage, increment,
                                           endOnError, endOnNull, endOnEmpty, endOnEmptyJson,
                                           ImmutableMap.copyOf(jsonEndValues));
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }
    public static @Nonnull Builder builder(@Nonnull String param) {
        return builder().withParam(param);
    }
    public static @Nonnull ParamPagingStrategy build(@Nonnull String param) {
        return builder(param).build();
    }

    @Override
    public @Nonnull Pager createPager() {
        return new ParamPager(param, firstPage, increment, endOnError, endOnNull,
                              endOnEmpty, endOnEmptyJson, jsonEndValues);
    }

    @Override
    public @Nonnull List<String> getParametersUsed() {
        return Collections.singletonList(param);
    }
}
