package br.ufsc.lapesd.freqel.reason.tbox.replacements;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public interface ReplacementGenerator {
    @Nullable TBox setTBox(@Nonnull TBox tBox);
    @Nonnull Iterable<Replacement> generate(@Nonnull CQuery query,
                                            @Nonnull TPEndpoint endpoint);
    @Nonnull List<Replacement> generateList(@Nonnull CQuery query,
                                            @Nonnull TPEndpoint endpoint);
}
