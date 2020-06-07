package br.ufsc.lapesd.riefederator.federation.spec.source;

import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.util.DictTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Set;

public interface SourceLoader {
    @Nonnull Set<String> names();

    /**
     * Loads the source described by the given sourceSpec.
     * @param sourceSpec Source specification
     * @param cacheDir Dir where the {@link Description} implementation may load/store information
     *                 This is given as a absolute File instance
     * @param referenceDir Relative file paths will be resolved relative to referenceDir
     * @return A set of non-null {@link Source} instances.
     * @throws SourceLoadException if sourceSpec has errors or an exception occurs during load
     * @throws IllegalArgumentException if the sourceSpec is not of a type supported by this
     *                                  {@link SourceLoader}
     */
    @Nonnull Set<Source> load(@Nonnull DictTree sourceSpec, @Nullable SourceCache cacheDir,
                              @Nonnull File referenceDir) throws SourceLoadException;
}
