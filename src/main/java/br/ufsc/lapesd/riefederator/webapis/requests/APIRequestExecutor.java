package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.NoTermSerializationException;
import com.google.errorprone.annotations.Immutable;
import org.glassfish.jersey.uri.UriTemplate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Set;

@Immutable
public interface APIRequestExecutor {
    /**
     * Get the set required bindings to call {@link #execute(Solution)}
     *
     * @return An unmodifiable {@link Set} of the binding names (not necessarily Atom names).
     */
    @Nonnull Set<String> getRequiredInputs();

    /**
     * Get the set of optional inputs that can be given to {@link #execute(Solution)}
     *
     * @return An unmodifiable {@link Set} of the binding names (not necessarily Atom names)
     */
    @Nonnull Set<String> getOptionalInputs();

    /**
     * A subset of inputs that may be missing in results.
     */
    @Nonnull Set<String> getInputsMissingInResult();

    default boolean hasInputs() {
        return !getRequiredInputs().isEmpty() || !getOptionalInputs().isEmpty();
    }

    /**
     * Executes an WebAPI skeleton request using the provided bindings.
     *
     * Typical use case is placing the bindings into an {@link UriTemplate}, but implementations
     * may perform more complex requests (e.g., sending payload in POST). The executor
     * implementation MAY perform multiple requests in response to a single #execute(Solution) call.
     *
     * The executor must return an iterator over endpoints where the definitive query can be
     * issued. Usually there will be at least one such endpoint in the results, but the iterator
     * may be empty if, for example the remote Web API returns an empty answer. In some
     * implementations, multiple {@link CQEndpoint} may be returned. In any case the
     * returned {@link Iterator} MUST be finite.
     *
     * @param input binds to use in the request
     * @return An iterator over {@link CQEndpoint} endpoints relevant to the given bindings
     * @throws MissingAPIInputsException if some required input is missing from given input bindings
     * @throws NoTermSerializationException if some terms cannot be serialized for use as inputs
     */
    @Nonnull Iterator<? extends CQEndpoint>
    execute(@Nonnull Solution input) throws MissingAPIInputsException, NoTermSerializationException;

    @Nullable HTTPRequestObserver setObserver(@Nonnull HTTPRequestObserver observer);
}
