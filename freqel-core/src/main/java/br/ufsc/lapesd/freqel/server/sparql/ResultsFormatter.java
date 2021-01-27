package br.ufsc.lapesd.freqel.server.sparql;

import br.ufsc.lapesd.freqel.query.results.Results;

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
