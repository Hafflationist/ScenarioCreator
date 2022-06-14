package de.mrobohm.utils;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StreamExtensions {

    private StreamExtensions() {
    }

    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> prepend(Stream<T> tail, T head) {
        return Stream.concat(Stream.of(head), tail);
    }

    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> replaceInStream(Stream<T> iterable, T originalElement, T newElement) {
        return replaceInStream(iterable, Stream.of(originalElement), newElement);
    }

    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> replaceInStream(Stream<T> iterable, T originalElement, Stream<T> newElementStream) {
        return replaceInStream(iterable, Stream.of(originalElement), newElementStream);
    }

    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> replaceInStream(Stream<T> iterable, Stream<T> originalElementStream, T newElement) {
        return replaceInStream(iterable, originalElementStream, Stream.of(newElement));
    }

    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> replaceInStream(Stream<T> stream, Stream<T> originalElementStream, Stream<T> newElementStream) {
        var originalElementSet = originalElementStream.collect(Collectors.toSet());
        var list = stream.toList();
        var firstHalf = list.stream().takeWhile(e -> !originalElementSet.contains(e)).toList();
        var secondHalf = list.stream()
                .dropWhile(e -> !originalElementSet.contains(e))
                .filter(e -> !originalElementSet.contains(e))
                .toList();
        return Stream.concat(Stream.concat(firstHalf.stream(), newElementStream), secondHalf.stream());
    }

    @NotNull
    public static <T, TException extends Throwable> T pickRandomOrThrow(Stream<T> stream, TException exception, Random random)
            throws TException {
        var list = stream.toList();
        var randomPickOption = list.stream()
                .skip(random.nextLong(list.size()))
                .findFirst();
        if (randomPickOption.isEmpty())
            throw exception;
        return randomPickOption.get();
    }

    @NotNull
    public static <T> Optional<T> tryPickRandom(Stream<T> stream, Random random) {
        return stream
                .skip(random.nextLong(stream.count()))
                .findFirst();
    }

    @NotNull
    public static <T, TException extends Throwable> Stream<T> pickRandomOrThrowMultiple(
            Stream<T> stream,
            int n,
            TException exception,
            Random random)
            throws TException {
        var list = stream.collect(Collectors.toList());
        Collections.shuffle(list, random); // Seltsame Magie (Unvorhersehbare SEITENEFFEKTE!) passiert hier.
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

    @Contract(pure = true)
    public static <T> Partition<T> partition(Stream<T> stream, Function<T, Boolean> predicate) {
        var list = stream.toList();
        var yes = list.stream().filter(predicate::apply);
        var no = list.stream().filter(e -> !predicate.apply(e));
        return new Partition<>(yes, no);
    }

    public record Partition<T>(Stream<T> yes, Stream<T> no) {
    }


}