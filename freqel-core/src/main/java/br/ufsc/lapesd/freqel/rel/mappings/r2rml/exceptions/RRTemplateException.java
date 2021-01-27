package br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions;

import javax.annotation.Nonnull;

public class RRTemplateException extends RRMappingException {
    private @Nonnull final String template;

    public RRTemplateException(@Nonnull String template, @Nonnull String message) {
        super(message + " template="+template);
        this.template = template;
    }

    public @Nonnull String getTemplate() {
        return template;
    }
}
