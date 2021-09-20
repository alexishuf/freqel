package br.ufsc.lapesd.freqel.reason.regimes;

import br.ufsc.lapesd.freqel.reason.rules.RuleSet;
import br.ufsc.lapesd.freqel.reason.rules.RuleSetRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.Collections.unmodifiableSet;

/**
 * A set of rules (and algorithms) that determine whether one graph
 * (including ABox and TBox) entails another.
 */
public class EntailmentRegime {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(EntailmentRegime.class);

    private final @Nonnull String iri, name, description;
    private final @Nullable String profileIRI;
    private final @Nonnull Collection<RuleSetRepresentation> ruleSetRepresentationRepresentations;
    private final @Nullable RuleSet ruleSet;

    public EntailmentRegime(@Nonnull String iri, @Nonnull String name,
                            @Nonnull String description, @Nullable String profileIRI,
                            @Nonnull Collection<RuleSetRepresentation> ruleSetRepresentationRepresentations,
                            @Nullable RuleSet ruleSet) {
        this.iri = iri;
        this.name = name;
        this.description = description;
        this.profileIRI = profileIRI;
        this.ruleSetRepresentationRepresentations = ruleSetRepresentationRepresentations;
        this.ruleSet = ruleSet;
    }

    public static @Nonnull EntailmentRegimeBuilder builder() {
        return new EntailmentRegimeBuilder();
    }

    /**
     * Get a {@link EntailmentRegimeBuilder} with all properties of this {@link EntailmentRegime},
     * excpet for the {@link RuleSetRepresentation}s, allowing a custom rule set to be set.
     *
     * @return a non-null {@link EntailmentRegimeBuilder} with same IRI, name and description
     */
    public @Nonnull EntailmentRegimeBuilder withoutRules() {
        return new EntailmentRegimeBuilder().iri(iri()).name(name()).description(description());
    }

    public static class EntailmentRegimeBuilder {
        protected @Nullable String iri, name, description, profileIRI;
        protected final @Nonnull Set<RuleSetRepresentation> ruleSetRepresentationRepresentations = new LinkedHashSet<>();
        protected @Nullable RuleSet ruleSet;

        public @Nonnull EntailmentRegimeBuilder iri(@Nullable String iri) {
            this.iri = iri;
            return this;
        }

        public @Nonnull EntailmentRegimeBuilder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        public @Nonnull EntailmentRegimeBuilder description(@Nullable String description) {
            this.description = description;
            return this;
        }

        public @Nonnull EntailmentRegimeBuilder profileIRI(@Nullable String profileIRI) {
            this.profileIRI = profileIRI;
            return this;
        }

        public @Nonnull EntailmentRegimeBuilder rulesRepresentation(@Nonnull Object mediaType,
                                                                    @Nonnull String content) {
            return rulesRepresentation(new RuleSetRepresentation(mediaType.toString(), content));
        }

        public @Nonnull EntailmentRegimeBuilder rulesRepresentation(@Nonnull RuleSetRepresentation ruleSetRepresentation) {
            this.ruleSetRepresentationRepresentations.add(ruleSetRepresentation);
            return this;
        }

        public @Nonnull EntailmentRegimeBuilder ruleSet(@Nullable RuleSet ruleSet) {
            this.ruleSet = ruleSet;
            return this;
        }

        public @Nonnull EntailmentRegime build() {
            return new EntailmentRegime(getIri(), getName(), getDescription(), profileIRI,
                                        unmodifiableSet(ruleSetRepresentationRepresentations),
                                        ruleSet);
        }

        protected @Nonnull String getIri() {
            return this.iri != null ? this.iri : "urn:uuid:"+ UUID.randomUUID();
        }
        protected @Nonnull String getName() {
            return this.name == null ? getIri() : name;
        }
        protected @Nonnull String getDescription() {
            return this.description == null ? getIri() : description;
        }
    }

    /**
     * A unique identifying IRI for this regime. W3C defines
     * <a href="https://www.w3.org/ns/entailment/">some</a> of such IRIs:
     *
     * @return A non-null IRI that uniquelly indetifies this instance.
     */
    public @Nonnull String iri() {
        return iri;
    }

    /**
     * A user-friendly (hopefully short) unique name that identifies this regime.
     *
     * @return a non-null and non-empty name.
     */
    public @Nonnull String name() {
        return name;
    }

    /**
     * A possibly long but readable description of this regime.
     *
     * @return non-null and non-empty description of this regime.
     */
    public @Nonnull String description() {
        return description;
    }

    /**
     * An OWL2 <a href="https://www.w3.org/ns/owl-profile/">profile IRI</a>, if applicable.
     *
     * @return a non-empty string with the OWL2 profile URI or null.
     */
    public @Nullable String profileIRI() {
        return profileIRI;
    }

    /**
     * The set of rules applied by this regime, in one or more representations.
     * <p>
     * An {@link EntailmentRegime} is not required to expose a {@link RuleSetRepresentation} even if it
     * is rule-based. Not providing RuleSets is justified if the regime (and this the rule set)
     * is well-known (i.e., standardized or assumed to be known by all relevant parties).
     *
     * @return A set of alternative representations of the same single set of rules applied by this regime, or an empty list if the regime is not rule based or if rules are well-known.
     */
    public @Nonnull Collection<RuleSetRepresentation> rulesRepresentations() {
        return ruleSetRepresentationRepresentations;
    }

    /**
     * Get the {@link RuleSet} implemented by this entailment regime.
     *
     * Even if an entailment regime is rule-based, the {@link RuleSet} may be unknown (and
     * thus null). If the set of rules is empty, then an empty {@link RuleSet} is returned.
     *
     * Returns of this method must be consitent with representations given in
     * {@link EntailmentRegime#rulesRepresentations()}.
     *
     * @return a possibly empty {@link RuleSet}, if known, or null
     */
    public @Nullable RuleSet rules() {
        return ruleSet;
    }

    public @Nonnull SourcedEntailmentRegime fromSingleSource() {
        return new SourcedEntailmentRegime(EntailmentEvidences.SINGLE_SOURCE, this);
    }

    public @Nonnull SourcedEntailmentRegime fromSingleSourceABox() {
        return new SourcedEntailmentRegime(EntailmentEvidences.SINGLE_SOURCE_ABOX, this);
    }

    public @Nonnull SourcedEntailmentRegime fromCrossSource() {
        return new SourcedEntailmentRegime(EntailmentEvidences.CROSS_SOURCE, this);
    }

    @Override public @Nonnull String toString() {
        return iri;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntailmentRegime)) return false;
        EntailmentRegime that = (EntailmentRegime) o;
        return iri.equals(that.iri) && name.equals(that.name)
                && description.equals(that.description) && Objects.equals(ruleSet, that.ruleSet);
    }

    @Override public int hashCode() {
        return Objects.hash(iri, name, description, ruleSet);
    }
}
