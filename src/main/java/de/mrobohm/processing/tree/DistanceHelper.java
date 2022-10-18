package de.mrobohm.processing.tree;

import de.mrobohm.data.Schema;
import de.mrobohm.heterogenity.Distance;

import java.util.List;
import java.util.SortedSet;

public final class DistanceHelper {
    private DistanceHelper() {
    }

    public static Distance avg(List<Distance> distanceList) {
        return new Distance(
                distanceList.stream()
                        .mapToDouble(Distance::structural)
                        .average()
                        .orElse(0.0),
                distanceList.stream()
                        .mapToDouble(Distance::linguistic)
                        .average()
                        .orElse(0.0),
                distanceList.stream()
                        .mapToDouble(Distance::constraintBased)
                        .average()
                        .orElse(0.0),
                distanceList.stream()
                        .mapToDouble(Distance::contextual)
                        .average()
                        .orElse(0.0)
        );
    }

    public static boolean isValid(List<Distance> distanceList, DistanceDefinition dtd, AggregationMethod aggr) {
        if (aggr.equals(AggregationMethod.CONJUNCTION)) {

            return distanceList.stream().allMatch(dtd::isValid);
        }
        final var avgDist = new Distance(
                distanceList.stream().mapToDouble(Distance::structural).average().orElse(0.0),
                distanceList.stream().mapToDouble(Distance::linguistic).average().orElse(0.0),
                distanceList.stream().mapToDouble(Distance::constraintBased).average().orElse(0.0),
                distanceList.stream().mapToDouble(Distance::contextual).average().orElse(0.0)
                );
        return dtd.isValid(avgDist);
    }

    public static List<Distance> distanceList(Schema schema, SortedSet<Schema> oldSchemaSet, DistanceMeasures measures) {
        return oldSchemaSet.stream()
                .map(oldSchema -> new Distance(
                        measures.structuralDistance().apply(schema, oldSchema),
                        measures.linguisticDistance().apply(schema, oldSchema),
                        measures.constraintBasedDistance().apply(schema, oldSchema),
                        measures.contextualDistance().apply(schema, oldSchema)
                ))
                .toList();
    }

    public enum AggregationMethod {
        CONJUNCTION,
        AVERAGE
    }
}