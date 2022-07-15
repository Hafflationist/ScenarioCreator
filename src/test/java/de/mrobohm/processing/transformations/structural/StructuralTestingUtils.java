package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdSimple;

import java.util.function.Function;
import java.util.stream.Stream;

public final class StructuralTestingUtils {
    private StructuralTestingUtils() {
    }

    public static Function<Integer, Id[]> getIdGenerator(int start) {
        return n -> Stream
                .iterate(start + 1, id -> id + 1)
                .limit(n)
                .map(IdSimple::new)
                .toArray(IdSimple[]::new);
    }
}
