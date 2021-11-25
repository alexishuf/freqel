package br.ufsc.lapesd.freqel.server.results;

import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import java.util.Iterator;

public class ChunkedEncoderUtils {
    public static @Nonnull Flux<Solution> toFlux(@Nonnull Results results) {
        return Flux.fromIterable(new Iterable<Solution>() {
            @Override public @Nonnull Iterator<Solution> iterator() {
                return results;
            }
        });
    }
}
