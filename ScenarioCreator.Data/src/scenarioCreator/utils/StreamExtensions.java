package scenarioCreator.utils;

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
    public static <T> Stream<T> postpend(Stream<T> init, T last) {
        return Stream.concat(init, Stream.of(last));
    }
    @Contract(pure = true)
    @NotNull
    public static <T> Stream<T> prepend(Stream<T> tail, T head) {
        return Stream.concat(Stream.of(head), tail);
    }

    @Contract(pure = true)
    @NotNull
    public static <T extends Comparable> Stream<T> replaceInStream(Stream<T> iterable, T originalElement, T newElement) {
        return replaceInStream(iterable, Stream.of(originalElement), newElement);
    }

    @Contract(pure = true)
    @NotNull
    public static <T extends Comparable> Stream<T> replaceInStream(
            Stream<T> iterable, T originalElement, Stream<T> newElementStream
    ) {
        return replaceInStream(iterable, Stream.of(originalElement), newElementStream);
    }

    @Contract(pure = true)
    @NotNull
    public static <T extends Comparable> Stream<T> replaceInStream(
            Stream<T> iterable, Stream<T> originalElementStream, T newElement
    ) {
        return replaceInStream(iterable, originalElementStream, Stream.of(newElement));
    }

    @Contract(pure = true)
    @NotNull
    public static <T extends Comparable> Stream<T> replaceInStream(
            Stream<T> stream, Stream<T> originalElementStream, Stream<T> newElementStream
    ) {
        final var originalElementSet = originalElementStream.collect(Collectors.toCollection(TreeSet::new));
        final var list = stream.toList();
        final var firstHalf = list.stream().takeWhile(e -> !originalElementSet.contains(e)).toList();
        final var secondHalf = list.stream()
                .dropWhile(e -> !originalElementSet.contains(e))
                .filter(e -> !originalElementSet.contains(e))
                .toList();
        return Stream.concat(Stream.concat(firstHalf.stream(), newElementStream), secondHalf.stream());
    }

    @NotNull
    public static <T, TException extends Throwable> T pickRandomOrThrow(Stream<T> stream, TException exception, Random random)
            throws TException {
        final var list = stream.toList();
        if (list.isEmpty()) {
            throw exception;
        }
        final var randomPickOption = list.stream()
                .skip(random.nextLong(list.size()))
                .findFirst();
        if (randomPickOption.isEmpty())
            throw exception;
        return randomPickOption.get();
    }

    @NotNull
    public static <T> Optional<T> tryPickRandom(Stream<T> stream, Random random) {
        final var list = stream.toList();
        if (list.isEmpty()) {
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
        final var list = stream.collect(Collectors.toList());
        Collections.shuffle(list, random); // Seltsame Magie (Unvorhersehbare SEITENEFFEKTE!) passiert hier.
        if (n > list.size()) {
            throw exception;
        }
        return list.subList(0, n).stream();
    }

    @Contract(pure = true)
    public static <T> Partition<T> partition(Stream<T> stream, Function<T, Boolean> predicate) {
        final var list = stream.toList();
        final var yes = list.stream().filter(predicate::apply);
        final var no = list.stream().filter(e -> !predicate.apply(e));
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

    public static <U, T> U foldLeft(Stream<T> stream, U seed, BiFunction<U, ? super T, U> folder) {
        final var list = stream.toList();
        if (list.isEmpty()) {
            return seed;
        }
        final var head = list.get(0);
        final var tail = list.stream().skip(1);
        final var newSeed = folder.apply(seed, head);
        return foldLeft(tail, newSeed, folder);
        // in the case of stack problems (TCO won't be performed in JAVA):
//        U result = seed;
//        for (T element : list) {
//            result = folder.apply(result, element);
//        }
//        return result;
    }
    public static <T> Stream<List<T>> split(Stream<T> stream, T delimiter) {
        final var list = stream.toList();
        if (list.isEmpty()) return Stream.of();
        final var head = list.stream().takeWhile(e -> !e.equals(delimiter)).toList();
        final var tailStream = list.stream().skip(head.size() + 1);
        final var tail = split(tailStream, delimiter);
        return prepend(tail, head);
    }

    public record Partition<T>(Stream<T> yes, Stream<T> no) {
    }
}