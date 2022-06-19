package de.mrobohm.operations.structural.generator;

import de.mrobohm.data.Schema;
import de.mrobohm.integrity.IdentificationNumberCalculator;

import java.util.Comparator;
import java.util.stream.Stream;

public final class IdentificationNumberGenerator {
    private IdentificationNumberGenerator() {
    }

    public static int[] generate(Schema schema, int n) {
        var idStream = IdentificationNumberCalculator.getAllIds(schema, true);
        var maxId = idStream.max(Comparator.naturalOrder()).orElse(1);
        return Stream.iterate(maxId + 1, id -> id + 1).limit(n).mapToInt(Integer::intValue).toArray();
    }
}