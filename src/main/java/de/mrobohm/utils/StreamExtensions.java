package de.mrobohm.utils;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        if (list.isEmpty()) {
            throw exception;
        }
        var randomPickOption = list.stream()
                .skip(random.nextLong(list.size()))
                .findFirst();
        if (randomPickOption.isEmpty())
            throw exception;
        return randomPickOption.get();
    }

    @NotNull
    public static <T> Optional<T> tryPickRandom(Stream<T> stream, Random random) {
        var list = stream.toList();
        if (list.isEmpty()){
            return Optional.empty();
        }
        return list.stream()
                .skip(random.nextLong(list.size()))
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
    public static <T> Partition<T> partition(Stream<T> stream, Function<T, Boolean> predicate) {
        var list = stream.toList();
        var yes = list.stream().filter(predicate::apply);
        var no = list.stream().filter(e -> !predicate.apply(e));
        return new Partition<>(yes, no);
    }

    public static <A, B, C> Stream<C> zip(Stream<? extends A> a,
                                          Stream<? extends B> b,
                                          BiFunction<? super A, ? super B, ? extends C> zipper) {
        Objects.requireNonNull(zipper);
        Spliterator<? extends A> aSpliterator = Objects.requireNonNull(a).spliterator();
        Spliterator<? extends B> bSpliterator = Objects.requireNonNull(b).spliterator();

        // Zipping looses DISTINCT and SORTED characteristics
        int characteristics = aSpliterator.characteristics() & bSpliterator.characteristics() &
                ~(Spliterator.DISTINCT | Spliterator.SORTED);

        long zipSize = ((characteristics & Spliterator.SIZED) != 0)
                ? Math.min(aSpliterator.getExactSizeIfKnown(), bSpliterator.getExactSizeIfKnown())
                : -1;

        Iterator<A> aIterator = Spliterators.iterator(aSpliterator);
        Iterator<B> bIterator = Spliterators.iterator(bSpliterator);
        Iterator<C> cIterator = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return aIterator.hasNext() && bIterator.hasNext();
            }

            @Override
            public C next() {
                return zipper.apply(aIterator.next(), bIterator.next());
            }
        };

        Spliterator<C> split = Spliterators.spliterator(cIterator, zipSize, characteristics);
        return (a.isParallel() || b.isParallel())
                ? StreamSupport.stream(split, true)
                : StreamSupport.stream(split, false);
    }

    public record Partition<T>(Stream<T> yes, Stream<T> no) {
    }
}