package br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RRMapping;

/**
 * An exception that occurs when evaluating one of {@link RRMapping} methods.
 *
 * This may be a issue with the R2RML mapping, but may as well be an issue with the
 * {@link RRMapping} client code.
 */
public class RRMappingException extends RRException {
    public RRMappingException(String message) {
        super(message);
    }
}
