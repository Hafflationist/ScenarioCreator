package de.mrobohm.utils;

import java.util.stream.Stream;

public final class StreamExtensions {

    public static <T> Stream<T> replaceInStream(Stream<T> iterable, T originalElement, T newElement) {
        return iterable.map(element -> element.equals(originalElement) ? newElement : element);
    }
}
