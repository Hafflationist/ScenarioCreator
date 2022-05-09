package de.mrobohm.utils;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StreamExtensions {

    private StreamExtensions() {}

    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> replaceInStream(Stream<T> iterable, T originalElement, Stream<T> newElements) {
        var firstHalf = iterable.takeWhile(e -> !e.equals(originalElement));
        var secondHalf = iterable
                .dropWhile(e -> !e.equals(originalElement))
                .filter(e -> !e.equals(originalElement));
        return Stream.concat(Stream.concat(firstHalf, newElements), secondHalf);
    }

    @NotNull
    public static <T, TException extends Throwable> T pickRandomOrThrow(Stream<T> stream, TException exception, Random random)
            throws TException {
        var randomPickOption = stream
                .skip(random.nextLong(stream.count()))
                .findFirst();
        if (randomPickOption.isEmpty())
            throw exception;
        return randomPickOption.get();
    }

    @NotNull
    public static <T, TException extends Throwable> Stream<T> pickRandomOrThrowMultiple(
            Stream<T> stream,
            int n,
            TException exception)
            throws TException {
        var list = stream.collect(Collectors.toList());
        Collections.shuffle(list); // Seltsame Magie (Unvorhersehbare SEITENEFFEKTE!) passiert hier.
        if (n > list.size()) {
            throw exception;
        }
        return list.subList(0, n).stream();
    }

    @Contract(pure = true)
    public static int getColumnId(Set<Table> tableSet) {
        var existingIdSet = tableSet.stream()
                .flatMap(t -> t.columnList().stream())
                .map(Column::id)
                .collect(Collectors.toSet());
        return Stream
                .iterate(0, x -> x + 1)
                .dropWhile(existingIdSet::contains)
                .toList()
                .get(0);
    }
}
