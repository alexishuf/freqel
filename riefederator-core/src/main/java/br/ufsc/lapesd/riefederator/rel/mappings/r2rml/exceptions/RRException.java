package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions;

public class RRException extends RuntimeException {
    public RRException() {
    }

    public RRException(String message) {
        super(message);
    }

    public RRException(String message, Throwable cause) {
        super(message, cause);
    }
}
