package br.ufsc.lapesd.riefederator.server.sparql;

import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.ws.rs.core.MediaType;
import java.util.Set;

public interface ResultsFormatter {
    @Nonnull Set<MediaType> outputMediaTypes();
    @Nonnull FormattedResults format(@Nonnull @WillClose Results results, boolean isAsk,
                                     @Nullable MediaType mediaType);
}
