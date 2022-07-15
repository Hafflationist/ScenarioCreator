package de.mrobohm.transformations.exceptions;

public class TransformationCouldNotBeExecutedException extends RuntimeException {
    public TransformationCouldNotBeExecutedException() {

    }

    public TransformationCouldNotBeExecutedException(String message) {
        super (message);
    }

    public TransformationCouldNotBeExecutedException(Throwable cause) {
        super (cause);
    }

    public TransformationCouldNotBeExecutedException(String message, Throwable cause) {
        super (message, cause);
    }
}
