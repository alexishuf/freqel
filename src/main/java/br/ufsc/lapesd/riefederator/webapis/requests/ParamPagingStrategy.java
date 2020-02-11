package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import com.google.errorprone.annotations.Immutable;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import java.util.Collections;
import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

@Immutable
public class ParamPagingStrategy implements PagingStrategy {
    private static final StdURI XSD_INT = new StdURI("http://www.w3.org/2001/XMLSchema#int");

    public static class ParamPager implements PagingStrategy.Pager {
        private @Nonnull String param;
        private int page;
        private final int increment;
        private boolean end;
        private final boolean endOnError, endOnNull, endOnEmptyResponse, endOnEmptyJson;

        public ParamPager(@Nonnull String param, int firstPage, int increment,
                          boolean endOnError, boolean endOnNull, boolean endOnEmptyResponse,
                          boolean endOnEmptyJson) {
            this.param = param;
            this.page = firstPage;
            this.increment = increment;
            this.end = false;
            this.endOnError = endOnError;
            this.endOnNull = endOnNull;
            this.endOnEmptyResponse = endOnEmptyResponse;
            this.endOnEmptyJson = endOnEmptyJson;
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
            if (endOnEmptyJson && response.getMediaType().isCompatible(APPLICATION_JSON_TYPE)) {
                response.bufferEntity();
                String json = response.readEntity(String.class);
                JsonElement e = new JsonParser().parse(json);
                if (e.isJsonObject() && e.getAsJsonObject().size() == 0) {
                    end = true;
                    return;
                }
                if (e.isJsonArray()) {
                    JsonArray array = e.getAsJsonArray();
                    if (array.size() == 0) {
                        end = true;
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
                        if (!allEmpty) end = true;
                    }
                }
            }
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

    protected ParamPagingStrategy(@Nonnull String param, int firstPage, int increment,
                                  boolean endOnError, boolean endOnNull, boolean endOnEmpty,
                                  boolean endOnEmptyJson) {
        this.param = param;
        this.firstPage = firstPage;
        this.increment = increment;
        this.endOnError = endOnError;
        this.endOnNull = endOnNull;
        this.endOnEmpty = endOnEmpty;
        this.endOnEmptyJson = endOnEmptyJson;
    }

    public static class Builder {
        private @Nonnull String param = "page";
        private int firstPage = 1, increment = 1;
        private boolean endOnError = true;
        private boolean endOnNull = true;
        private boolean endOnEmpty = true;
        private boolean endOnEmptyJson = false;

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

        public @Nonnull Builder withEndOnEmptyJson(boolean endOnEmptyJson) {
            this.endOnEmptyJson = endOnEmptyJson;
            return this;
        }

        public @Nonnull ParamPagingStrategy build() {
            return new ParamPagingStrategy(param, firstPage, increment,
                                           endOnError, endOnNull, endOnEmpty, endOnEmptyJson);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }
    public static @Nonnull Builder builder(@Nonnull String param) {
        return new Builder().withParam(param);
    }

    @Override
    public @Nonnull Pager createPager() {
        return new ParamPager(param, firstPage, increment, endOnError, endOnNull, endOnEmpty, endOnEmptyJson);
    }

    @Override
    public @Nonnull List<String> getParametersUsed() {
        return Collections.singletonList(param);
    }
}
