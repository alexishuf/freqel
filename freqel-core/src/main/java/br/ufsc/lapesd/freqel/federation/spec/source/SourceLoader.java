package br.ufsc.lapesd.freqel.federation.spec.source;

import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.BackoffStrategy;
import br.ufsc.lapesd.freqel.util.DictTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Set;

public interface SourceLoader {
    @Nonnull Set<String> names();

    /**
     * Set a temporary directory to store downloads or to create temporary files that
     * may be too large for the system default temporary dir.
     *
     * @param tempDir the temporary dir.
     */
    void setTempDir(@Nonnull File tempDir);

    /**
     * Set a {@link SourceCache} instance to be used by {@link #load(DictTree, File)} to
     * reuse previously constructed description metadata for a source.
     *
     * @param sourceCache a {@link SourceCache} instance.
     */
    void setSourceCache(@Nullable SourceCache sourceCache);

    /**
     * Set a {@link BackoffStrategy} to apply if queries sent as part of a-priori indexing fail.
     *
     * @param strategy the {@link BackoffStrategy} to apply in case of failed indexing queries
     */
    void setIndexingBackoffStrategy(@Nonnull BackoffStrategy strategy);

    /**
     * Loads the source described by the given sourceSpec.
     * @param sourceSpec Source specification
     * @param referenceDir Relative file paths will be resolved relative to referenceDir
     * @return A set of non-null {@link TPEndpoint} instances.
     * @throws SourceLoadException if sourceSpec has errors or an exception occurs during load
     * @throws IllegalArgumentException if the sourceSpec is not of a type supported by this
     *                                  {@link SourceLoader}
     */
    @Nonnull Set<TPEndpoint> load(@Nonnull DictTree sourceSpec, @Nonnull File referenceDir) throws SourceLoadException;
}
