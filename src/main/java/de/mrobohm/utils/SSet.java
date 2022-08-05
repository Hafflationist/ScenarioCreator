package de.mrobohm.utils;

import java.util.Arrays;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SSet {
    private SSet() {
    }

    @SafeVarargs
    public static <T> SortedSet<T> of(T... content) {
        return Arrays.stream(content).collect(Collectors.toCollection(TreeSet::new));
    }

    public static <T> SortedSet<T> concat(SortedSet<T> set1, SortedSet<T> set2) {
        return Stream.concat(set1.stream(), set2.stream()).collect(Collectors.toCollection(TreeSet::new));
    }

    public static <T> SortedSet<T> concat(SortedSet<T> set, Stream<T> stream) {
        return Stream.concat(set.stream(), stream).collect(Collectors.toCollection(TreeSet::new));
    }

    public static <T> Set<SortedSet<T>> powerSet(SortedSet<T> set) {
        assert set.size() <= 22 : "Calculating the power set would exceed the capabilities of my computer. : (";
        var list = set.stream().toList();

        var possibleIdxList = Stream
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
}
