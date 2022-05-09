package de.mrobohm.operations.exceptions;

public class NoTableFoundException extends Exception {
    public NoTableFoundException() {

    }

    public NoTableFoundException(String message) {
        super (message);
    }

    public NoTableFoundException(Throwable cause) {
        super (cause);
    }

    public NoTableFoundException(String message, Throwable cause) {
        super (message, cause);
    }
}
