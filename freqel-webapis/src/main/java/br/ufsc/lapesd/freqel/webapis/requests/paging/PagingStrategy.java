package br.ufsc.lapesd.freqel.webapis.requests.paging;

import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.results.Solution;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.List;

@Immutable
public interface PagingStrategy {
    interface Pager {
        interface State { }

        @Nonnull State getPagerState();
        void setPagerState(@Nonnull State state);
        boolean atEnd();
        @Nonnull Solution apply(@Nonnull Solution solution);
        void notifyResponse(@Nonnull Response response);
        void notifyResponseEndpoint(@Nullable CQEndpoint endpoint);
    }

    @Nonnull Pager createPager();
    @Nonnull List<String> getParametersUsed();
}
