package br.ufsc.lapesd.freqel.reason.tbox;

import javax.annotation.Nonnull;

public interface TBoxMaterializer extends AutoCloseable, TBox {
    /**
     * Loads the sources into the reasoner.
     *
     * This method discards any previously loaded TBox. This may also cause pre-materialization
     * by the reasoner. What is pre-materialized (and if something is prematerialized) is up to
     * the {@link TBoxMaterializer} implementation.
     *
     * @param sources source TBox
     */
    void load(@Nonnull TBoxSpec sources);

}
