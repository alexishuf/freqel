package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public interface APIDescriptionParser {
    @Nonnull APIDescriptionParserFactory createFactory();

    /**
     * Get the fallback context used by this parser. The object can be modified.
     */
    @Nonnull APIDescriptionContext getFallbackContext();

    /**
     * Get a list of endpoints names (usually paths) listed in this description.
     *
     * Names in this list can be used with the get*() methods.
     */
    @Nonnull Collection<String> getEndpoints();

    /**
     * Get the {@link Molecule} that describes the successful response from the given endpoint
     *
     * @param endpoint endpoint name {@see #getEndpoints()}
     * @param ctx context that will override {@link #getFallbackContext()} and the description.
     * @throws NoSuchElementException if endpoint is not in {@link #getEndpoints()}.
     * @return The {@link Molecule}.
     */
    @Nonnull Molecule getMolecule(@Nonnull String endpoint, @Nullable APIDescriptionContext ctx);

    /**
     * Get the {@link APIMolecule} that describes the successful response from the given endpoint.
     *
     * @param endpoint endpoint name {@see #getEndpoints()}
     * @param ctx context that will override {@link #getFallbackContext()} and the description.
     * @throws NoSuchElementException if endpoint is not in {@link #getEndpoints()}.
     * @return The {@link APIMolecule}.
     */

    @Nonnull APIMolecule getAPIMolecule(@Nonnull String endpoint,
                                        @Nullable APIDescriptionContext ctx);

    /**
     * Get the {@link WebAPICQEndpoint} that describes the successful response from the
     * given endpoint.
     *
     * @param endpoint endpoint name {@see #getEndpoints()}
     * @param ctx context that will override {@link #getFallbackContext()} and the description.
     * @throws NoSuchElementException if endpoint is not in {@link #getEndpoints()}.
     * @return The {@link WebAPICQEndpoint}.
     */

    @Nonnull WebAPICQEndpoint getEndpoint(@Nonnull String endpoint,
                                          @Nullable APIDescriptionContext ctx);


    default @Nonnull Molecule getMolecule(@Nonnull String endpoint) {
        return getMolecule(endpoint, null);
    }
    default @Nonnull APIMolecule getAPIMolecule(@Nonnull String endpoint) {
        return getAPIMolecule(endpoint, null);
    }
    default @Nonnull WebAPICQEndpoint getEndpoint(@Nonnull String endpoint) {
        return getEndpoint(endpoint, null);
    }

    /**
     * Get a map with all endpoint names and the result of <code>getMolecule(name, ctx)</code>.
     */
    default @Nonnull Map<String, Molecule>
    getAllMolecules(@Nullable APIDescriptionContext ctx) {
        Map<String, Molecule> map = new HashMap<>();
        for (String name : getEndpoints())
            map.put(name, getMolecule(name, ctx));
        return map;
    }

    /**
     * Get a map with all endpoint names and the result of <code>getAPIMolecule(name, ctx)</code>.
     */
    default @Nonnull Map<String, APIMolecule>
    getAllAPIMolecules(@Nullable APIDescriptionContext ctx) {
        Map<String, APIMolecule> map = new HashMap<>();
        for (String name : getEndpoints())
            map.put(name, getAPIMolecule(name, ctx));
        return map;
    }

    /**
     * Get a map with all endpoint names and the result of <code>getEndpoint(name, ctx)</code>.
     */
    default @Nonnull Map<String, WebAPICQEndpoint>
    getAllEndpoints(@Nullable APIDescriptionContext ctx) {
        Map<String, WebAPICQEndpoint> map = new HashMap<>();
        for (String name : getEndpoints())
            map.put(name, getEndpoint(name, ctx));
        return map;
    }

}
