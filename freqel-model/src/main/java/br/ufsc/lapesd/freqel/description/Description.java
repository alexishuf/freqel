package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.query.CQuery;

import javax.annotation.Nonnull;

public interface Description {
    /**
     * Gets a {@link CQueryMatch} for the subset of query that matches this {@link Description};
     */
    @Nonnull CQueryMatch match(@Nonnull CQuery query, @Nonnull MatchReasoning reasoning);

    /**
     * Same as {@link #match(CQuery, MatchReasoning)}, but will only-return a non-null result
     * if all matching can be done using only local data (i.e., no requests will be sent to
     * the source, if it is remove).
     *
     * @param query the conjunctive query to match
     * @return non-null if all information for a {@link #match(CQuery, MatchReasoning)} call
     *         was present locally, else returns null (unknown if there is a match or not).
     */
    @Nonnull CQueryMatch localMatch(@Nonnull CQuery query, @Nonnull MatchReasoning reasoning);

    /**
     * Indicates whether {@link #match(CQuery, MatchReasoning)} and
     * {@link #localMatch(CQuery, MatchReasoning)} support the given reasoning mode.
     *
     * @param mode The {@link MatchReasoning} mode.
     * @return true if reasoning is performed at the given mode or if the mode flag would be
     *         ignored.
     */
    boolean supports(@Nonnull MatchReasoning mode);

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
     * implementation requires the initialization for
     * {@link Description#match(CQuery, MatchReasoning)}, calling match before
     * {@link Description#waitForInit(int)} would return true will simply yield an
     * empty match (i.e., no errors).
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
