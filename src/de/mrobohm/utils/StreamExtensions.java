package de.mrobohm.utils;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.stream.Stream;

public final class StreamExtensions {

    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> replaceInStream(Stream<T> iterable, T originalElement, Stream<T> newElements) {
        var firstHalf = iterable.takeWhile(e -> !e.equals(originalElement));
        var secondHalf = iterable
                .dropWhile(e -> !e.equals(originalElement))
                .filter(e -> !e.equals(originalElement));
        return Stream.concat(Stream.concat(firstHalf, newElements), secondHalf);
    }

    @Contract(pure = true)
    @NotNull
    public static <T, TException extends Throwable> T pickRandomOrThrow(Stream<T> stream, TException exception)
            throws TException {
        var randomPickOption = stream
                .skip(new Random().nextLong(stream.count()))
                .findFirst();
        if (randomPickOption.isEmpty())
            throw exception;
        return randomPickOption.get();
    }
}
