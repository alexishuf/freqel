package br.ufsc.lapesd.freqel.query.endpoint;

public class EndpointCloseException extends RuntimeException {
    public EndpointCloseException(String message) {
        super(message);
    }

    public EndpointCloseException(String message, Throwable cause) {
        super(message, cause);
    }
}
