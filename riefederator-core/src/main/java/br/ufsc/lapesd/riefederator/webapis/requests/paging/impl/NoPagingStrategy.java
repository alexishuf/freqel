package br.ufsc.lapesd.riefederator.webapis.requests.paging.impl;

import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import com.google.errorprone.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

@Immutable
public class NoPagingStrategy implements PagingStrategy {
    static class NoPagingPager implements PagingStrategy.Pager, PagingStrategy.Pager.State  {
        private boolean end = false;

        @Override public @Nonnull State getPagerState() {
            return this;
        }

        @Override public void setPagerState(@NotNull PagingStrategy.Pager.State state) {
            if (!(state instanceof NoPagingPager))
                throw new IllegalArgumentException();
            end = ((NoPagingPager) state).end;
        }

        @Override
        public boolean atEnd() {
            return end;
        }

        @Override
        public @Nonnull Solution apply(@Nonnull Solution solution) {
            return solution;
        }

        @Override
        public void notifyResponse(@Nonnull Response ignored) {
            end = true;
        }

        @Override
        public void notifyResponseEndpoint(@Nullable CQEndpoint ignored) {
            end = true;
        }
    }

    public static final @Nonnull NoPagingStrategy INSTANCE = new NoPagingStrategy();

    @Override
    public @Nonnull Pager createPager() {
        return new NoPagingPager();
    }

    @Override
    public @Nonnull List<String> getParametersUsed() {
        return Collections.emptyList();
    }
}
