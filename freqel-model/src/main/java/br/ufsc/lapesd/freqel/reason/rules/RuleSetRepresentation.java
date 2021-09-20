package br.ufsc.lapesd.freqel.reason.rules;

import javax.annotation.Nonnull;
import java.util.Objects;

public class RuleSetRepresentation {
    private final @Nonnull String mediaType;
    private final @Nonnull String content;

    public RuleSetRepresentation(@Nonnull String mediaType, @Nonnull String content) {
        this.mediaType = mediaType;
        this.content = content;
    }

    /**
     * A media type for the rules given in {@link RuleSetRepresentation#content()}.
     *
     * This should be specific enough to allow proper selection of a rule parser/converter.
     *
     * @return An internet media type identifying the rule syntax used in {@link RuleSetRepresentation#content()}
     */
    public @Nonnull String mediaType() { return mediaType; }

    /**
     * A representation of a set of rules as a string.
     *
     * @return A non-null set of rules in {@link RuleSetRepresentation#mediaType()} syntax.
     */
    public @Nonnull String content() { return content; }

    @Override public String toString() {
        String shortened = content.substring(0, Math.min(160, content.length()));
        return String.format("[%s]{%s%s}", mediaType, shortened,
                             shortened.length() < content.length() ? "..." : "");
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuleSetRepresentation)) return false;
        RuleSetRepresentation ruleSetRepresentation = (RuleSetRepresentation) o;
        return mediaType.equals(ruleSetRepresentation.mediaType) && content.equals(ruleSetRepresentation.content);
    }

    @Override public int hashCode() {
        return Objects.hash(mediaType, content);
    }
}
