package processing.transformations.constraintBased.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.*;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.FunctionalDependency;
import scenarioCreator.generation.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import scenarioCreator.utils.SSet;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

class FunctionalDependencyManagerTest {


    private FunctionalDependency of(int a, int b) {
        return new FunctionalDependency(SSet.of(new IdSimple(a)), SSet.of(new IdSimple(b)));
    }

    private FunctionalDependency of(Id a, Id b) {
        return new FunctionalDependency(SSet.of(a), SSet.of(b));
    }

    private List<Column> fromIds(Id... ids) {
        return Arrays.stream(ids).map(id -> (Column) new ColumnLeaf(
                id,
                new StringPlusNaked("", Language.Technical),
                DataType.getRandom(new Random()),
                ColumnContext.getDefault(),
                SSet.of()
        )).toList();
    }

    @Test
    void getValidFdSetMerge() {
        // --- Arrange
        final var fdSetValid = SSet.of(
                of(2, 3),
                of(3, 4),
                new FunctionalDependency(
                        SSet.of(new IdSimple(4)),
                        SSet.of(new IdSimple(1), new IdSimple(2))
                ),
                new FunctionalDependency(
                        SSet.of(new IdSimple(4)),
                        SSet.of(new IdSimple(1), new IdSimple(3))
                )
        );
        final var fdSetInvalid = SSet.of(
                of(3, 1),
                of(3, 2),
                of(1, 2),
                new FunctionalDependency(
                        SSet.of(new IdSimple(4)),
                        SSet.of(new IdSimple(1))
                )
        );
        final var fdSet = SSet.concat(fdSetInvalid, fdSetValid);
        final var afterTransColumnList = fromIds(
                new IdMerge(new IdSimple(1), new IdSimple(2), MergeOrSplitType.Xor),
                new IdSimple(3), new IdSimple(4), new IdSimple(5), new IdSimple(6)
        );

        // --- Act
        final var afterTransFdSet = FunctionalDependencyManager.getValidFdSet(fdSet, afterTransColumnList);

        // --- Assert
        Assertions.assertEquals(fdSetValid.size(), afterTransFdSet.size());
    }

    @Test
    void getValidFdSetPart() {
        // --- Arrange
        final var fdSetValid = SSet.of(
                of(3, 1),
                of(3, 2),
                of(1, 4) // because of XOR
        );
        final var fdSetInvalid = SSet.of(
                of(2, 4),
                of(2, 1)
        );
        final var fdSet = SSet.concat(fdSetInvalid, fdSetValid);
        final var afterTransColumnList = fromIds(
                new IdPart(new IdSimple(1), 0, MergeOrSplitType.Xor),
                new IdPart(new IdSimple(2), 0, MergeOrSplitType.And),
                new IdSimple(3), new IdSimple(4), new IdSimple(5), new IdSimple(6)
        );

        // --- Act
        final var afterTransFdSet = FunctionalDependencyManager.getValidFdSet(fdSet, afterTransColumnList);

        // --- Assert
        Assertions.assertEquals(fdSetValid.size(), afterTransFdSet.size());
    }

    @Test
    void attributeClosure() {
        // --- Arrange
        final var fdSet = SSet.of(
                of(1, 2),
                of(2, 3),
                of(3, 1),
                of(4, 5)
        );
        final var attr = new IdSimple(1);

        // --- Act
        final var attrClosure = FunctionalDependencyManager.attributeClosure(SSet.of(attr), fdSet);

        // --- Assert
        Assertions.assertTrue(attrClosure.contains(new IdSimple(2)));
        Assertions.assertTrue(attrClosure.contains(new IdSimple(3)));
        Assertions.assertFalse(attrClosure.contains(new IdSimple(4)));
        Assertions.assertFalse(attrClosure.contains(new IdSimple(5)));
    }

    @Test
    void minimalCover1() {
        // --- Arrange
        final var fdSet = SSet.of(
                of(1, 2),
                of(2, 3),
                of(1, 3),
                of(4, 5)
        );
        final var coverExpected1 = SSet.of(
                of(1, 2),
                of(1, 3),
                of(4, 5)
        );
        final var coverExpected2 = SSet.of(
                of(1, 2),
                of(2, 3),
                of(4, 5)
        );

        // --- Act
        final var coverActual = FunctionalDependencyManager.minimalCover(fdSet);

        // --- Assert
        Assertions.assertEquals(coverExpected1.size(), coverActual.size());
        Assertions.assertTrue(coverActual.equals(coverExpected1) || coverActual.equals(coverExpected2));
    }

    @Test
    void minimalCover2() {
        // --- Arrange
        final var fdSet = SSet.of(
                of(1, 2),
                of(1, 3),
                new FunctionalDependency(
                        SSet.of(new IdSimple(1)),
                        SSet.of(new IdSimple(2), new IdSimple(3))
                )
        );
        final var coverExpected1 = SSet.of(
                of(1, 2),
                of(1, 3)
        );
        final var coverExpected2 = SSet.of(
                new FunctionalDependency(
                        SSet.of(new IdSimple(1)),
                        SSet.of(new IdSimple(2), new IdSimple(3))
                )
        );

        // --- Act
        final var coverActual = FunctionalDependencyManager.minimalCover(fdSet);

        // --- Assert
        Assertions.assertTrue(coverActual.equals(coverExpected1) || coverActual.equals(coverExpected2));
    }
}