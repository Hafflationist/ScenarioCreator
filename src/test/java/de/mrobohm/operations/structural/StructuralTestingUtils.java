package de.mrobohm.operations.structural;

import java.util.function.Function;
import java.util.stream.Stream;

public final class StructuralTestingUtils {
    private StructuralTestingUtils() {
    }

    public static Function<Integer, int[]> getIdGenerator(int start) {
        return n -> Stream.iterate(start + 1, id -> id + 1).limit(n).mapToInt(Integer::intValue).toArray();
    }
}
