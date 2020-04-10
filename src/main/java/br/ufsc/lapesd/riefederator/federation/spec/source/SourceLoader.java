package br.ufsc.lapesd.riefederator.federation.spec.source;

import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.util.DictTree;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Set;

public interface SourceLoader {
    @Nonnull Set<String> names();

    /**
     * Loads the source described by the given sourceSpec.
     * @param sourceSpec Source specification
     * @param referenceDir Relative file paths will be resolved relative to referenceDir
     * @return A set of non-null {@link Source} instances.
     * @throws SourceLoadException if sourceSpec has errors or an exception occurs during load
     * @throws IllegalArgumentException if the sourceSpec is not of a type supported by this
     *                                  {@link SourceLoader}
     */
    @Nonnull Set<Source> load(@Nonnull DictTree sourceSpec,
                              @Nonnull File referenceDir) throws SourceLoadException;
}
