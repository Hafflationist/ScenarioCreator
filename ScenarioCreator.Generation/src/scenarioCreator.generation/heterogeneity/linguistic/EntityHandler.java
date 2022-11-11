package scenarioCreator.generation.heterogeneity.linguistic;

import scenarioCreator.data.Entity;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class EntityHandler {
    private EntityHandler() {
    }


    public static List<Pair<StringPlus, List<StringPlus>>> getNameMapping(
            SortedSet<Entity> entitySet1, SortedSet<Entity> entitySet2, SortedSet<Id> intersectingIdSet
    ) {
        return entitySet1.stream()
                .filter(e1 -> !intersectingIdSet.contains(e1.id()))
                .map(e1 -> new Pair<>(
                                e1.name(),
                                entitySet2.stream()
                                        .filter(e2 -> {
                                            final var e1IdSet = IdentificationNumberCalculator
                                                    .extractIdSimple(e1.id())
                                                    .collect(Collectors.toSet());
                                            return IdentificationNumberCalculator
                                                    .extractIdSimple(e2.id())
                                                    .anyMatch(e1IdSet::contains);
                                        })
                                        .map(Entity::name)
                                        .toList()
                        )
                )
                .toList();
    }

    public static double mappingToDistance(
            List<Pair<StringPlus, List<StringPlus>>> mapping, BiFunction<StringPlus, StringPlus, Double> diff
    ) {
        // Hier muss noch beachtet werden, was mit doppelt gezählten Differenzen passiert!
        final var deterministicRandom = new Random(0);
        return mapping.stream()
                .mapToDouble(pair -> {
                    final var e = pair.first();
                    final var mapped = pair.second();
                    final var concatenatedStringOpt = mapped.stream()
                            .reduce((a, b) -> LinguisticUtils.merge(a, b, deterministicRandom));
                    if (concatenatedStringOpt.isEmpty()) {
                        return 0.0;
                    }
                    // Die Idee hinter dem aktuellen Gewicht: Jedes Element, das über eine 1:1-Korrespondenz hinaus geht,
                    // wird nur in der einen Hälfte berücksichtigt.
                    final var weight = 1.0 + ((mapped.size() - 1.0) / 2.0);
                    return weight * diff.apply(e, concatenatedStringOpt.get());
                })
                .sum();
    }
}