package de.mrobohm.utils;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class SSet {
    private SSet() {
    }

    @SafeVarargs
    public static <T> SortedSet<T> of(T ... content) {
        return Arrays.stream(content).collect(Collectors.toCollection(TreeSet::new));
    }
}
