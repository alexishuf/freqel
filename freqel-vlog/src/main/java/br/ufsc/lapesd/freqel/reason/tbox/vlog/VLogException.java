package br.ufsc.lapesd.freqel.reason.tbox.vlog;

public class VLogException extends RuntimeException {
    public VLogException(String message, Throwable cause) {
        super(message, cause);
    }

    public VLogException(Throwable cause) {
        super(cause);
    }

    public VLogException(String message) {
        super(message);
    }
}
