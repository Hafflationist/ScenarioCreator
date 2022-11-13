package scenarioCreator.generation.heterogeneity.linguistic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.data.Entity;
import scenarioCreator.data.Language;
import scenarioCreator.data.identification.*;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.stream.Stream;

class EntityHandlerTest {

    private static EntityMock em(Id id) {
        final var name = new StringPlusNaked(id.toString(), Language.Technical);
        return new EntityMock(name, id);
    }

    private static double parseDoubleOrDefault(String str, double defaultValue) {
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static List<StringPlus> get(List<Pair<StringPlus, List<StringPlus>>> mapping, StringPlus key) {
      return mapping.stream()
              .filter(pair -> pair.first().equals(key))
              .findFirst()
              .map(Pair::second)
              .orElse(List.of());
    }

    @Test
    void getNameMapping() {
        // --- Arrange
        final var merge56 = em(
                new IdMerge(new IdSimple(5), new IdSimple(6), MergeOrSplitType.Other)
        );
        final var entitySet1 = SSet.<Entity>of(
                em(new IdSimple(1)),
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
        final var entitySet2 = SSet.<Entity>of(
                em(new IdSimple(1)),
                em(new IdSimple(2)),
                merge34,
                em(new IdSimple(5)),
                em(new IdSimple(6)),
                em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other)),
                em(new IdSimple(8))
        );
        final var intersectingIdSet = SSet.<Id>of(new IdSimple(1));

        // --- Act
        final var mapping = EntityHandler.getNameMapping(entitySet1, entitySet2, intersectingIdSet);

        // --- Assert
        Assertions.assertEquals(entitySet1.size() - 1, mapping.size());
        Assertions.assertEquals(
                List.of(em(new IdSimple(2)).name),
                get(mapping, em(new IdSimple(2)).name)
        );
        Assertions.assertEquals(
                List.of(merge34.name),
                get(mapping, em(new IdSimple(3)).name)
        );
        Assertions.assertEquals(
                List.of(merge34.name),
                get(mapping, em(new IdSimple(4)).name)
        );
        Assertions.assertEquals(
                List.of(em(new IdSimple(5)).name, em(new IdSimple(6)).name),
                get(mapping, merge56.name)
        );
        Assertions.assertEquals(
                List.of(
                        em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other)).name,
                        em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other)).name
                ),
                get(mapping, em(new IdSimple(7)).name)
        );
        Assertions.assertEquals(
                List.of(em(new IdSimple(8)).name),
                get(mapping, em(new IdPart(new IdSimple(8), 0, MergeOrSplitType.Other)).name)
        );
        Assertions.assertEquals(
                List.of(em(new IdSimple(8)).name),
                get(mapping, em(new IdPart(new IdSimple(8), 1, MergeOrSplitType.Other)).name)
        );
    }

    @Test
    void mappingToDistanceTestSummation() {
        // --- Arrange
        final var mapping = List.of(
                new Pair<>(spnDist(0.0), List.of(spnDist(1.0))),
                new Pair<>(spnDist(0.1), List.of(spnDist(1.0))),
                new Pair<>(spnDist(0.2), List.of(spnDist(1.0))),
                new Pair<>(spnDist(0.3), List.of(spnDist(3.0)))
        );

        // --- Act
        final var dist = EntityHandler.mappingToDistance(mapping, this::diff);

        // --- Assert
        final var abs = Math.abs(dist - 6.0);
        Assertions.assertTrue(abs < 1e-3);
    }

    @ParameterizedTest
    @ValueSource(ints = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
    })
    void mappingToDistanceTestWeight(int size) {
        // --- Arrange
        final var mapped = (List<StringPlus>) Stream
                .iterate(65, x -> x + 1)
                .map(Character::toString)
                .map(this::spnDist)
                .limit(size)
                .toList();
        final var mapping = List.of(
                new Pair<>(spnDist(1.0), mapped)
        );

        // --- Act
        final var dist = EntityHandler.mappingToDistance(mapping, this::diff);

        // --- Assert
        final var expected = (size - 1) / 2.0 + 1.0;
        final var abs = Math.abs(dist - expected);
        Assertions.assertTrue(abs < 1e-3);
    }

    private double diff(StringPlus sp1, StringPlus sp2) {
        final var str1 = sp1.rawString(LinguisticUtils::merge).replaceAll("[^\\d.-]", "");
        final var str2 = sp2.rawString(LinguisticUtils::merge).replaceAll("[^\\d.-]", "");
        final var dist1 = parseDoubleOrDefault(str1, -Double.MAX_VALUE);
        final var dist2 = parseDoubleOrDefault(str2, -Double.MAX_VALUE);
        return Math.max(dist1, dist2);
    }

    private StringPlus spnDist(double dist) {
        return new StringPlusNaked(Double.toString(dist), Language.Technical);
    }

    private StringPlus spnDist(String name) {
        return new StringPlusNaked(name, Language.Technical);
    }

    private record EntityMock(StringPlus name, Id id) implements Entity {
    }
}