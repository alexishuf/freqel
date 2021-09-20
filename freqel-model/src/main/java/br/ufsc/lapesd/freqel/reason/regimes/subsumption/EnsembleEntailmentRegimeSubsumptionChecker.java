package br.ufsc.lapesd.freqel.reason.regimes.subsumption;

import br.ufsc.lapesd.freqel.reason.regimes.EntailmentRegime;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class EnsembleEntailmentRegimeSubsumptionChecker
        implements EntailmentRegimeSubsumptionChecker {
    private final @Nonnull List<EntailmentRegimeSubsumptionChecker> list;

    public EnsembleEntailmentRegimeSubsumptionChecker(
            @Nonnull List<EntailmentRegimeSubsumptionChecker> list) {
        this.list = list;
    }

    public EnsembleEntailmentRegimeSubsumptionChecker() {
        this(loadImplementations());
    }

    private static @Nonnull List<EntailmentRegimeSubsumptionChecker> loadImplementations() {
        ServiceLoader<EntailmentRegimeSubsumptionChecker> loader;
        loader = ServiceLoader.load(EntailmentRegimeSubsumptionChecker.class);
        List<EntailmentRegimeSubsumptionChecker> list = new ArrayList<>();
        for (EntailmentRegimeSubsumptionChecker checker : loader)
            list.add(checker);
        return list;
    }

    @Override
    public @Nonnull Boolean subsumes(@Nonnull EntailmentRegime subsumer,
                                     @Nonnull EntailmentRegime subsumed) {
        for (EntailmentRegimeSubsumptionChecker checker : list) {
            Boolean result = checker.subsumes(subsumer, subsumed);
            if (result != null)
                return result;
        }
        return subsumer.equals(subsumed);
    }
}
