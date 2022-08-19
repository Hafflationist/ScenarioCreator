package de.mrobohm.heterogenity.linguistic;

import de.mrobohm.data.Entity;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;

import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class EntityHandler {
    private EntityHandler() {
    }


    public static Map<Entity, TreeSet<Entity>> getEntityMapping(
            SortedSet<Entity> entitySet1, SortedSet<Entity> entitySet2, SortedSet<Id> intersectingIdSet
    ) {
        return entitySet1.stream()
                .filter(e1 -> !intersectingIdSet.contains(e1.id()))
                .collect(Collectors.toMap(
                        Function.identity(),
                        e1 -> entitySet2.stream()
                                .filter(e2 -> {
                                    var e1IdSet = IdentificationNumberCalculator
                                            .extractIdSimple(e1.id())
                                            .collect(Collectors.toSet());
                                    return IdentificationNumberCalculator
                                            .extractIdSimple(e2.id())
                                            .anyMatch(e1IdSet::contains);
                                })
                                .collect(Collectors.toCollection(TreeSet::new))
                ));
    }

    public static double mappingToDistance(
            Map<Entity, TreeSet<Entity>> mapping, BiFunction<StringPlus, StringPlus, Double> diff
    ) {
        var deterministicRandom = new Random(0);
        return mapping.keySet().stream()
                .mapToDouble(e -> {
                    var mapped = mapping.get(e);
                    var concatenatedStringOpt = mapped.stream()
                            .map(Entity::name)
                            .reduce((a, b) -> LinguisticUtils.merge(a, b, deterministicRandom));
                    if (concatenatedStringOpt.isEmpty()) {
                        return 0.0;
                    }
                    // Die Idee hinter dem aktuellen Gewicht: Jedes Element, das über eine 1:1-Korrespondenz hinaus geht,
                    // wird nur in der einen Hälfte berücksichtigt.
                    var weight = 1.0 + ((mapped.size() - 1.0) / 2.0);
                    return weight * diff.apply(e.name(), concatenatedStringOpt.get());
                })
                .sum();
    }
}