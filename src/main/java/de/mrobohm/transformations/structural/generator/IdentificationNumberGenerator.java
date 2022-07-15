package de.mrobohm.transformations.structural.generator;

import de.mrobohm.data.Schema;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.integrity.IdentificationNumberCalculator;

import java.util.Comparator;
import java.util.stream.Stream;

public final class IdentificationNumberGenerator {
    private IdentificationNumberGenerator() {
    }

    public static Id[] generate(Schema schema, int n) {
        var idStream = IdentificationNumberCalculator.getAllIds(schema, true);
        var maxId = IdentificationNumberCalculator
                .extractIdSimple(idStream)
                .map(IdSimple::number)
                .max(Comparator.naturalOrder())
                .orElse(1);
        return Stream
                .iterate(maxId + 1, id -> id + 1)
                .limit(n)
                .map(IdSimple::new)
                .toArray(IdSimple[]::new);
    }
}