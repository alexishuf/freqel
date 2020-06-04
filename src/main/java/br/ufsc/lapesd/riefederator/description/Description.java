package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

public interface Description {
    /**
     * Gets a {@link CQueryMatch} for the subset of query that matches this {@link Description};
     */
    @Nonnull CQueryMatch match(@Nonnull CQuery query);

    /**
     * If possible updates the description. For some sources this may cause querying
     * to the underlying endpoint.
     *
     * If the operation is executed, this method SHOULD run the operation asynchronously and
     * return ASAP with the operation in background.
     */
    void update();

    /**
     * Same as update(), but only starts an update operation if no update was ever performed.
     *
     * Use {@link Description#waitForInit(int)} to ensure initialization is complete. If the
     * implementation requires the initialization for {@link Description#match(CQuery)},
     * calling match before {@link Description#waitForInit(int)} would return true will
     * simply yield an empty match (i.e., no errors).
     */
    void init();

    /**
     * Wait until the timeout expires or the init() task finishes.
     *
     * @return true iff init() async task completed.
     */
    boolean waitForInit(int timeoutMilliseconds);

    /**
     * Same as update(), but ensures the update is complete before returning.
     *
     * @param timeoutMilliseconds maximum wait time, in milliseconds
     * @return true iff the operation completed.
     */
    boolean updateSync(int timeoutMilliseconds);

}
