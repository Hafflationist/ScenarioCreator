package de.mrobohm.heterogenity.linguistic;

import de.mrobohm.data.Entity;
import de.mrobohm.data.Language;
import de.mrobohm.data.identification.*;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class EntityHandlerTest {

    private static EntityMock em(Id id) {
        var name = new StringPlusNaked(id.toString(), Language.Technical);
        return new EntityMock(name, id);
    }

    private static double parseDoubleOrDefault(String str, double defaultValue) {
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Test
    void getNameMapping() {
        // --- Arrange
        var merge56 = em(
                new IdMerge(new IdSimple(5), new IdSimple(6), MergeOrSplitType.Other)
        );
        var entitySet1 = SSet.<Entity>of(
                em(new IdSimple(1)),
                em(new IdSimple(2)),
                em(new IdSimple(3)),
                em(new IdSimple(4)),
                merge56,
                em(new IdSimple(7)),
                em(new IdPart(new IdSimple(8), 0, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(8), 1, MergeOrSplitType.Other))
        );
        var merge34 = em(
                new IdMerge(new IdSimple(3), new IdSimple(4), MergeOrSplitType.Other)
        );
        var entitySet2 = SSet.<Entity>of(
                em(new IdSimple(1)),
                em(new IdSimple(2)),
                merge34,
                em(new IdSimple(5)),
                em(new IdSimple(6)),
                em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other)),
                em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other)),
                em(new IdSimple(8))
        );
        var intersectingIdSet = SSet.<Id>of(new IdSimple(1));

        // --- Act
        var mapping = EntityHandler.getNameMapping(entitySet1, entitySet2, intersectingIdSet);

        // --- Assert
        Assertions.assertEquals(entitySet1.size() - 1, mapping.keySet().size());
        Assertions.assertEquals(
                SSet.of(em(new IdSimple(2)).name),
                mapping.get(em(new IdSimple(2)).name)
        );
        Assertions.assertEquals(
                SSet.of(merge34.name),
                mapping.get(em(new IdSimple(3)).name)
        );
        Assertions.assertEquals(
                SSet.of(merge34.name),
                mapping.get(em(new IdSimple(4)).name)
        );
        Assertions.assertEquals(
                SSet.of(em(new IdSimple(5)).name, em(new IdSimple(6)).name),
                mapping.get(merge56.name)
        );
        Assertions.assertEquals(
                SSet.of(
                        em(new IdPart(new IdSimple(7), 0, MergeOrSplitType.Other)).name,
                        em(new IdPart(new IdSimple(7), 1, MergeOrSplitType.Other)).name
                ),
                mapping.get(em(new IdSimple(7)).name)
        );
        Assertions.assertEquals(
                SSet.of(em(new IdSimple(8)).name),
                mapping.get(em(new IdPart(new IdSimple(8), 0, MergeOrSplitType.Other)).name)
        );
        Assertions.assertEquals(
                SSet.of(em(new IdSimple(8)).name),
                mapping.get(em(new IdPart(new IdSimple(8), 1, MergeOrSplitType.Other)).name)
        );
    }

    @Test
    void mappingToDistanceTestSummation() {
        // --- Arrange
        var mapping = Map.of(
                spnDist(0.0), SSet.of(spnDist(1.0)),
                spnDist(0.1), SSet.of(spnDist(1.0)),
                spnDist(0.2), SSet.of(spnDist(1.0)),
                spnDist(0.3), SSet.of(spnDist(3.0))
        );

        // --- Act
        var dist = EntityHandler.mappingToDistance(mapping, this::diff);

        // --- Assert
        var abs = Math.abs(dist - 6.0);
        Assertions.assertTrue(abs < 1e-3);
    }

    @ParameterizedTest
    @ValueSource(ints = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
    })
    void mappingToDistanceTestWeight(int size) {
        // --- Arrange
        var mapped = (SortedSet<StringPlus>) Stream
                .iterate(65, x -> x + 1)
                .map(Character::toString)
                .map(this::spnDist)
                .limit(size)
                .collect(Collectors.toCollection(TreeSet::new));
        var mapping = Map.of(
                spnDist(1.0), mapped
        );

        // --- Act
        var dist = EntityHandler.mappingToDistance(mapping, this::diff);

        // --- Assert
        var expected = (size - 1) / 2.0 + 1.0;
        var abs = Math.abs(dist - expected);
        Assertions.assertTrue(abs < 1e-3);
    }

    private double diff(StringPlus sp1, StringPlus sp2) {
        var str1 = sp1.rawString().replaceAll("[^\\d.-]", "");
        var str2 = sp2.rawString().replaceAll("[^\\d.-]", "");
        var dist1 = parseDoubleOrDefault(str1, -Double.MAX_VALUE);
        var dist2 = parseDoubleOrDefault(str2, -Double.MAX_VALUE);
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