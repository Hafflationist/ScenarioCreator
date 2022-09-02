package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.*;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        var fdSetValid = SSet.of(
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
        var fdSetInvalid = SSet.of(
                of(3, 1),
                of(3, 2),
                of(1, 2),
                new FunctionalDependency(
                        SSet.of(new IdSimple(4)),
                        SSet.of(new IdSimple(1))
                )
        );
        var fdSet = SSet.concat(fdSetInvalid, fdSetValid);
        var afterTransColumnList = fromIds(
                new IdMerge(new IdSimple(1), new IdSimple(2), MergeOrSplitType.Xor),
                new IdSimple(3), new IdSimple(4), new IdSimple(5), new IdSimple(6)
        );

        // --- Act
        var afterTransFdSet = FunctionalDependencyManager.getValidFdSet(fdSet, afterTransColumnList);

        // --- Assert
        Assertions.assertEquals(fdSetValid.size(), afterTransFdSet.size());
    }

    @Test
    void getValidFdSetPart() {
        // --- Arrange
        var fdSetValid = SSet.of(
                of(3, 1),
                of(3, 2),
                of(1, 4) // because of XOR
        );
        var fdSetInvalid = SSet.of(
                of(2, 4),
                of(2, 1)
        );
        var fdSet = SSet.concat(fdSetInvalid, fdSetValid);
        var afterTransColumnList = fromIds(
                new IdPart(new IdSimple(1), 0, MergeOrSplitType.Xor),
                new IdPart(new IdSimple(2), 0, MergeOrSplitType.And),
                new IdSimple(3), new IdSimple(4), new IdSimple(5), new IdSimple(6)
        );

        // --- Act
        var afterTransFdSet = FunctionalDependencyManager.getValidFdSet(fdSet, afterTransColumnList);

        // --- Assert
        Assertions.assertEquals(fdSetValid.size(), afterTransFdSet.size());
    }

    @Test
    void attributeClosure() {
        // --- Arrange
        var fdSet = SSet.of(
                of(1, 2),
                of(2, 3),
                of(3, 1),
                of(4, 5)
        );
        var attr = new IdSimple(1);

        // --- Act
        var attrClosure = FunctionalDependencyManager.attributeClosure(SSet.of(attr), fdSet);

        // --- Assert
        Assertions.assertTrue(attrClosure.contains(new IdSimple(2)));
        Assertions.assertTrue(attrClosure.contains(new IdSimple(3)));
        Assertions.assertFalse(attrClosure.contains(new IdSimple(4)));
        Assertions.assertFalse(attrClosure.contains(new IdSimple(5)));
    }

    @Test
    void minimalCover1() {
        // --- Arrange
        var fdSet = SSet.of(
                of(1, 2),
                of(2, 3),
                of(1, 3),
                of(4, 5)
        );
        var coverExpected1 = SSet.of(
                of(1, 2),
                of(1, 3),
                of(4, 5)
        );
        var coverExpected2 = SSet.of(
                of(1, 2),
                of(2, 3),
                of(4, 5)
        );

        // --- Act
        var coverActual = FunctionalDependencyManager.minimalCover(fdSet);

        // --- Assert
        Assertions.assertEquals(coverExpected1.size(), coverActual.size());
        Assertions.assertTrue(coverActual.equals(coverExpected1) || coverActual.equals(coverExpected2));
    }

    @Test
    void minimalCover2() {
        // --- Arrange
        var fdSet = SSet.of(
                of(1, 2),
                of(1, 3),
                new FunctionalDependency(
                        SSet.of(new IdSimple(1)),
                        SSet.of(new IdSimple(2), new IdSimple(3))
                )
        );
        var coverExpected1 = SSet.of(
                of(1, 2),
                of(1, 3)
        );
        var coverExpected2 = SSet.of(
                new FunctionalDependency(
                        SSet.of(new IdSimple(1)),
                        SSet.of(new IdSimple(2), new IdSimple(3))
                )
        );

        // --- Act
        var coverActual = FunctionalDependencyManager.minimalCover(fdSet);

        // --- Assert
        Assertions.assertTrue(coverActual.equals(coverExpected1) || coverActual.equals(coverExpected2));
    }
}