package scenarioCreator.generation.heterogeneity.constraintBased;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import scenarioCreator.data.Entity;
import scenarioCreator.data.Language;
import scenarioCreator.data.identification.*;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class BasedConstraintBasedBaseTest {


    private static EntityMock em(Id id) {
        final var name = new StringPlusNaked(id.toString(), Language.Technical);
        return new EntityMock(name, id);
    }

    @Test
    void findCorrespondingEntityPairs() {
        // --- Arrange
        final var merge56 = em(
                new IdMerge(new IdSimple(5), new IdSimple(6), MergeOrSplitType.Other)
        );
        final var entityStream1 = Stream.<Entity>of(
                em(new IdSimple(-1)),
                em(new IdSimple(2)),
                em(new IdSimple(3)),
                em(new IdSimple(4)),
                merge56,
                em(new IdSimple(7)),
                em(new IdPart(new IdSimple(8), 0, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(8), 1, MergeOrSplitType.Other))
        );
        final var merge34 = em(
                new IdMerge(new IdSimple(3), new IdSimple(4), MergeOrSplitType.Other)
        );
        final var entityStream2 = Stream.<Entity>of(
                em(new IdSimple(1)),
                em(new IdSimple(2)),
                merge34,
                em(new IdSimple(5)),
                em(new IdSimple(6)),
                em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other)),
                em(new IdSimple(8))
        );

        // --- Act
        final var correspondenceSet = BasedConstraintBasedBase
                .findCorrespondingEntityPairs(entityStream1, entityStream2)
                .collect(Collectors.toCollection(TreeSet::new));

        // --- Assert
        final var expectedCorrespondenceSet = SSet.of(
                new Pair<Entity, Entity>(em(new IdSimple(2)), em(new IdSimple(2))),
                new Pair<Entity, Entity>(em(new IdSimple(3)), merge34),
                new Pair<Entity, Entity>(em(new IdSimple(4)), merge34),
                new Pair<Entity, Entity>(merge56, em(new IdSimple(5))),
                new Pair<Entity, Entity>(merge56, em(new IdSimple(6))),
                new Pair<Entity, Entity>(em(new IdSimple(7)), em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other))),
                new Pair<Entity, Entity>(em(new IdSimple(7)), em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other))),
                new Pair<Entity, Entity>(em(new IdPart(new IdSimple(8), 0, MergeOrSplitType.Other)), em(new IdSimple(8))),
                new Pair<Entity, Entity>(em(new IdPart(new IdSimple(8), 1, MergeOrSplitType.Other)), em(new IdSimple(8)))
        );
        Assertions.assertEquals(expectedCorrespondenceSet.size(), correspondenceSet.size());
        Assertions.assertTrue(correspondenceSet.containsAll(expectedCorrespondenceSet));
    }

    @Test
    void findCorrespondingEntityPairs2() {
        // --- Arrange
        final var entityStream1 = Stream.<Entity>of(
                em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(8), 0, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(8), 1, MergeOrSplitType.Other))
        );
        final var merge78 = em(
                new IdMerge(new IdSimple(7), new IdSimple(8), MergeOrSplitType.Other)
        );
        final var entityStream2 = Stream.<Entity>of(
                merge78
        );

        // --- Act
        final var correspondenceSet = BasedConstraintBasedBase
                .findCorrespondingEntityPairs(entityStream1, entityStream2)
                .collect(Collectors.toCollection(TreeSet::new));

        // --- Assert
        final var expectedCorrespondenceSet = SSet.of(
                new Pair<Entity, Entity>(em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other)), merge78),
                new Pair<Entity, Entity>(em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other)), merge78),
                new Pair<Entity, Entity>(em(new IdPart(new IdSimple(8), 0, MergeOrSplitType.Other)), merge78),
                new Pair<Entity, Entity>(em(new IdPart(new IdSimple(8), 1, MergeOrSplitType.Other)), merge78)
        );
        Assertions.assertEquals(expectedCorrespondenceSet.size(), correspondenceSet.size());
        Assertions.assertTrue(correspondenceSet.containsAll(expectedCorrespondenceSet));
    }

    private record EntityMock(StringPlus name, Id id) implements Entity {
        @Override
        public String toString() {
            return "EntityMock-" + id.toString();
        }
    }
}