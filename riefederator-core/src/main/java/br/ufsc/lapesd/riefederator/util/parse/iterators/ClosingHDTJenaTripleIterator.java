package br.ufsc.lapesd.riefederator.util.parse.iterators;

import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

public class ClosingHDTJenaTripleIterator extends HDTJenaTripleIterator {
    private static final Logger logger = LoggerFactory.getLogger(ClosingHDTJenaTripleIterator.class);
    private final @Nonnull HDT hdt;

    public ClosingHDTJenaTripleIterator(@Nullable Object source, @Nonnull IteratorTripleString it,
                                        @Nonnull HDT hdt) {
        super(source, it);
        this.hdt = hdt;
    }

    @Override public void close() {
        try {
            hdt.close();
        } catch (IOException e) {
            logger.error("IOException closing HDT object from source {}. Ignoring", source, e);
        }
    }
}
