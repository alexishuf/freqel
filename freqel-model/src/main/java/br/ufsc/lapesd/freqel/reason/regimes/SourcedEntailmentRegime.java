package br.ufsc.lapesd.freqel.reason.regimes;

import br.ufsc.lapesd.freqel.reason.regimes.subsumption.EnsembleEntailmentRegimeSubsumptionChecker;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class SourcedEntailmentRegime {
    private final @Nonnull EntailmentEvidences evidences;
    private final @Nonnull EntailmentRegime regime;
    private static EnsembleEntailmentRegimeSubsumptionChecker subsumptionChecker;

    public SourcedEntailmentRegime(@Nonnull EntailmentEvidences evidences,
                                   @Nonnull EntailmentRegime regime) {
        this.evidences = evidences;
        this.regime = regime;
    }

    /**
     * Parses a string as a {@link SourcedEntailmentRegime}.
     *
     * Strings most conform to the following pseudo regular expression: (E@)?I, where:
     * <ul>
     *     <li><strong>E</strong> is a {@link EntailmentEvidences#name()}, case
     *         insensitive and optioannly with '_' replaced with '-'</li>
     *     <li><strong>I</strong> is an IRI of the {@link EntailmentRegime}. If it is an
     *         IRI minted by W3C, the built-in instances from {@link W3CEntailmentRegimes}
     *         will be used.</li>
     * </ul>
     *
     * The strings produced by {@link SourcedEntailmentRegime#toString()} conform to these rules.
     *
     * @param string a string representation of a {@link SourcedEntailmentRegime}
     * @param defaultEvidences If there is no "E@" prefix as discussed above, assume that the
     *        {@link SourcedEntailmentRegime} described by the string uses this
     *        {@link EntailmentEvidences} strategy. If this parameter is null, parsing strings
     *        with no explicit {@link EntailmentEvidences} will fail.
     * @return A {@link SourcedEntailmentRegime}
     * @throws IllegalArgumentException if the string is an invalid or is ambiguous (no IRI or
     *         no {@link EntailmentEvidences} when no default is given)
     */
    public static @Nonnull SourcedEntailmentRegime
    fromString(@Nonnull String string,
               @Nullable EntailmentEvidences defaultEvidences) throws IllegalArgumentException {
        string = string.trim();
        if (string.isEmpty() || string.equals("@")) {
            throw new IllegalArgumentException("The empty string does not represent " +
                                               "a SourcedEntailmentRegime");
        }
        String[] pieces = string.split("@");
        EntailmentEvidences evidences = defaultEvidences;
        if (pieces.length >= 2) {
            try {
                String name = pieces[0].toUpperCase().trim().replace("-", "_");
                evidences = EntailmentEvidences.valueOf(name);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("EntailmentEvidences name: "+pieces[0]);
            }
        }
        if (evidences == null) {
            throw new IllegalArgumentException("No \"<EntailmentEvidences>@\" prefix " +
                                               "and no default set.");
        }
        String uri = string.substring(string.indexOf("@")+1);
        try {
            URI parsed = new URI(uri);
            if (!parsed.isAbsolute())
                throw new IllegalArgumentException("Non-absolute EntailmentRegime IRI");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Malformed URI "+uri+": "+e.getMessage());
        }
        EntailmentRegime regime = W3CEntailmentRegimes.getByIRI(uri);
        if (regime == null)
            regime = EntailmentRegime.builder().iri(uri).build();
        return new SourcedEntailmentRegime(evidences, regime);
    }

    public @Nonnull EntailmentEvidences evidences() { return evidences; }
    public @Nonnull EntailmentRegime regime() { return regime; }

    public boolean subsumes(@Nonnull SourcedEntailmentRegime other) {
        if (!evidences().subsumes(other.evidences()))
            return false;
        if (subsumptionChecker == null)
            subsumptionChecker = new EnsembleEntailmentRegimeSubsumptionChecker();
        return subsumptionChecker.subsumes(regime(), other.regime());
    }

    public @Nonnull String toString() {
        return evidences().name()+"@"+regime().name();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SourcedEntailmentRegime)) return false;
        SourcedEntailmentRegime that = (SourcedEntailmentRegime) o;
        return evidences == that.evidences && regime.equals(that.regime);
    }

    @Override public int hashCode() {
        return Objects.hash(evidences, regime);
    }
}
