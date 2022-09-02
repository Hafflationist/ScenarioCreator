package de.mrobohm.processing.transformations.constraintBased;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.processing.transformations.structural.StructuralTestingUtils;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;

class FunctionalDependencyRemoverTest {

    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
            30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
            40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
            50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
            70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
            80, 81, 82, 83, 84, 85, 86, 87, 88, 89
    })
    void transform(int seed) {
        // --- Arrange
        var random = new Random(seed);
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column5 = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column6 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column7 = new ColumnLeaf(new IdSimple(7), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column8 = new ColumnLeaf(new IdSimple(8), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column9 = new ColumnLeaf(new IdSimple(9), name, dataType, ColumnContext.getDefault(), SSet.of());

        var table = StructuralTestingUtils.createTable(
                101,
                List.of(column1, column2, column3, column4, column5, column6, column7, column8, column9),
                random
        );
        var tableSet = SSet.of(table);
        var schema = new Schema(new IdSimple(1001), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var idGenerator = StructuralTestingUtils.getIdGenerator(600);
        var transformation = new FunctionalDependencyRemover();

        // --- Act
        var newTableSet = transformation.transform(table, idGenerator, random);

        // --- Assert
        var newSchema = new Schema(new IdSimple(1001), name, Context.getDefault(), newTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
        Assertions.assertEquals(1, newTableSet.size());
        var newTable = newTableSet.first();
        var fdSetClosure = FunctionalDependencyManager.transClosure(
                table.functionalDependencySet()
        );
        var newFdSetClosure = FunctionalDependencyManager.transClosure(
                newTable.functionalDependencySet()
        );
        Assertions.assertTrue(newFdSetClosure.size() < fdSetClosure.size());
        Assertions.assertTrue(fdSetClosure.containsAll(newFdSetClosure));
    }

    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
            30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
            40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
            50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
            70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
            80, 81, 82, 83, 84, 85, 86, 87, 88, 89
    })
    void transform2(int seed) {
        // --- Arrange
        var random = new Random(seed);
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(), SSet.of());

        var table = new Table(
                new IdSimple(101),
                name,
                List.of(column1, column2, column3),
                Context.getDefault(),
                SSet.of(),
                SSet.of(
                        new FunctionalDependency(SSet.of(column1.id()), SSet.of(column2.id())),
                        new FunctionalDependency(SSet.of(column2.id()), SSet.of(column3.id())),
                        new FunctionalDependency(SSet.of(column1.id()), SSet.of(column3.id())) // redundant
                )
        );
        var idGenerator = StructuralTestingUtils.getIdGenerator(600);
        var transformation = new FunctionalDependencyRemover();

        // --- Act
        var newTableSet = transformation.transform(table, idGenerator, random);

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        var newTable = newTableSet.first();
        var fdSetClosure = FunctionalDependencyManager.transClosure(
                table.functionalDependencySet(), false
        ); // hier müssten 2 zurückgegeben werden
        var newFdSetClosure = FunctionalDependencyManager.transClosure(
                newTable.functionalDependencySet(), false
        ); // hier nur eine
        Assertions.assertTrue(newFdSetClosure.size() < fdSetClosure.size());
        Assertions.assertTrue(fdSetClosure.containsAll(newFdSetClosure));
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var column3 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(14), name, dataType, ColumnContext.getDefault(), SSet.of());

        var validTable = StructuralTestingUtils.createTable(
                101, List.of(column1, column2, column3, column4)
        );
        var invalidTable = new Table(
                new IdSimple(102), name, List.of(column1, column2, column3, column4),
                Context.getDefault(), SSet.of(), SSet.of()
        );
        var tableSet = SSet.of(validTable, invalidTable);
        var transformation = new FunctionalDependencyRemover();

        // --- Act
        var candidateSet = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, candidateSet.size());
        Assertions.assertTrue(candidateSet.contains(validTable));
    }
}