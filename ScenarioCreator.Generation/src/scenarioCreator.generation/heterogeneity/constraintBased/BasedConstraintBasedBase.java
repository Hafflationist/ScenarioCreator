package scenarioCreator.generation.heterogeneity.constraintBased;

import scenarioCreator.data.Entity;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.utils.Pair;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class BasedConstraintBasedBase {
    private BasedConstraintBasedBase(){}

    public static <TEntity extends Entity> Stream<Pair<TEntity, TEntity>> findCorrespondingEntityPairs(
            Stream<TEntity> entityStream1, Stream<TEntity> entityStream2
    ) {
        final var entityList2 = entityStream2.toList();
        return entityStream1
                .filter(t1 -> IdentificationNumberCalculator.extractIdSimple(t1.id()).count() <= 2)
                .flatMap(t1 -> entityList2.stream()
                        .filter(t2 -> {
                            final var idList2 = IdentificationNumberCalculator
                                    .extractIdSimple(t2.id())
                                    .collect(Collectors.toSet());
                            if (idList2.size() > 2) return false;
                            return IdentificationNumberCalculator.extractIdSimple(t1.id())
                                    .anyMatch(idList2::contains);

                        })
                        .map(t2 -> new Pair<>(t1, t2))
                );
    }
}