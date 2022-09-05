package de.mrobohm.utils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SSet {
    private SSet() {
    }

    @SafeVarargs
    public static <T> SortedSet<T> of(T... content) {
        return Arrays.stream(content).collect(Collectors.toCollection(TreeSet::new));
    }

    public static <T> SortedSet<T> concat(Collection<T> col1, Collection<T> col2) {
        return Stream.concat(col1.stream(), col2.stream()).collect(Collectors.toCollection(TreeSet::new));
    }

    public static <T> SortedSet<T> concat(SortedSet<T> set, Stream<T> stream) {
        return Stream.concat(set.stream(), stream).collect(Collectors.toCollection(TreeSet::new));
    }

    public static <T> SortedSet<T> prepend(T head, SortedSet<T> set) {
        return Stream.concat(Stream.of(head), set.stream()).collect(Collectors.toCollection(TreeSet::new));
    }

    public static <T> Set<SortedSet<T>> powerSet(SortedSet<T> set) {
        assert set.size() <= 22 : "Calculating the power set would exceed the capabilities of my computer. : (";
        final var list = set.stream().toList();

        final var possibleIdxList = Stream
                .iterate(0, x -> x + 1)
                .limit(list.size())
                .toList();
        return Stream
                .iterate(0L, x -> x + 1L)
                .limit(0b1L << list.size())
                .map(x -> possibleIdxList.stream()
                        .filter(idx -> ((0b1L << idx) & x) > 0)
                        .map(list::get)
                        .collect(Collectors.toCollection(TreeSet::new)))
                .collect(Collectors.toSet());
    }

    public static <U, T> U foldLeft(SortedSet<T> set, U seed, BiFunction<U, ? super T, U> folder) {
        if (set.isEmpty()){
            return seed;
        }
        final var head = set.first();
        final var tail = set.stream().skip(1).collect(Collectors.toCollection(TreeSet::new));
        final var newSeed = folder.apply(seed, head);
        return foldLeft(tail, newSeed, folder);
        // in the case of stack problems (TCO won't be performed in JAVA):
//        U result = seed;
//        for (T element : set) {
//            result = folder.apply(result, element);
//        }
//        return result;
    }
}