package scenarioCreator.generation.processing.transformations.structural.generator;

import scenarioCreator.data.Schema;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;

import java.util.Comparator;
import java.util.stream.Stream;

public final class IdentificationNumberGenerator {
    private IdentificationNumberGenerator() {
    }

    public static Id[] generate(Schema schema, int n) {
        final var idStream = IdentificationNumberCalculator.getAllIds(schema, true);
        final var maxId = IdentificationNumberCalculator
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