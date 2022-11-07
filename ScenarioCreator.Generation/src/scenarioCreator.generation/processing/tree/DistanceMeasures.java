package scenarioCreator.generation.processing.tree;

import scenarioCreator.data.Schema;

import java.util.function.BiFunction;

public record DistanceMeasures(
        BiFunction<Schema, Schema, Double> structuralDistance,
        BiFunction<Schema, Schema, Double> linguisticDistance,
        BiFunction<Schema, Schema, Double> constraintBasedDistance,
        BiFunction<Schema, Schema, Double> contextualDistance
) {
    public static DistanceMeasures getDefault() {
        final BiFunction<Schema, Schema, Double> defaultDistance = (a, b) -> 0.1;
        return new DistanceMeasures(
                defaultDistance, defaultDistance, defaultDistance, defaultDistance
        );
    }
}