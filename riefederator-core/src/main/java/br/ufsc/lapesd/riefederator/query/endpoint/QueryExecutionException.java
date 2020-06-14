package br.ufsc.lapesd.riefederator.query.endpoint;

public class QueryExecutionException extends RuntimeException {
    public QueryExecutionException(String message) {
        super(message);
    }

    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryExecutionException(Throwable cause) {
        super(cause);
    }
}
