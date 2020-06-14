package br.ufsc.lapesd.riefederator.linkedator;

import br.ufsc.lapesd.riefederator.linkedator.strategies.LinkedatorStrategy;
import br.ufsc.lapesd.riefederator.model.term.std.TemplateLink;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class LinkedatorResult {
    private final @Nonnull TemplateLink templateLink;
    private final @SuppressWarnings("Immutable") @Nullable LinkedatorStrategy strategy;
    private final double confidence;

    public LinkedatorResult(@Nonnull TemplateLink templateLink,
                            @Nullable LinkedatorStrategy strategy, double confidence) {
        checkArgument(templateLink.getSubject().isVar(), "subject of TemplateLink must be a Var");
        checkArgument(templateLink.getObject().isVar(), "object of TemplateLink must be a Var");
        this.templateLink = templateLink;
        this.strategy = strategy;
        this.confidence = confidence;
    }

    public LinkedatorResult(@Nonnull TemplateLink templateLink, double confidence) {
        this(templateLink, null, confidence);
    }

    public @Nonnull TemplateLink getTemplateLink() {
        return templateLink;
    }

    public @Nullable LinkedatorStrategy getStrategy() {
        return strategy;
    }

    public double getConfidence() {
        return confidence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LinkedatorResult)) return false;
        LinkedatorResult that = (LinkedatorResult) o;
        return Double.compare(that.getConfidence(), getConfidence()) == 0 &&
                getTemplateLink().equals(that.getTemplateLink());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTemplateLink(), getConfidence());
    }

}
