package de.mrobohm.transformations.exceptions;

public class NoColumnFoundException extends Exception {
    public NoColumnFoundException () {

    }

    public NoColumnFoundException (String message) {
        super (message);
    }

    public NoColumnFoundException (Throwable cause) {
        super (cause);
    }

    public NoColumnFoundException (String message, Throwable cause) {
        super (message, cause);
    }
}
