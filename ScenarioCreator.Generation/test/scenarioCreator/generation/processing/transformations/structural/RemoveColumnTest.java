package scenarioCreator.generation.processing.transformations.structural;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;

class RemoveColumnTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var invalidColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(7))));
        final var validTable = new Table(
                new IdSimple(101), name, List.of(validColumn, invalidColumn), Context.getDefault(), SSet.of(), SSet.of()
        );
        final var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        final var transformation = new RemoveColumn();

        // --- Act
        final var newTableList = transformation.transform(validTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newTableList.size());
        final var newTable = newTableList.first();
        Assertions.assertEquals(1, newTable.columnList().size());
    }

    @Test
    void getCandidatesShouldHandleNormalCase() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(7))));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(8))));
        final var invalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(9))));
        final var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());

        final var invalidTable1 = new Table(new IdSimple(101), name, List.of(invalidColumn1, invalidColumn3), Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable2 = new Table(new IdSimple(102), name, List.of(invalidColumn2, invalidColumn1), Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable3 = new Table(new IdSimple(103), name, List.of(invalidColumn3, invalidColumn2), Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable4 = new Table(new IdSimple(104), name, List.of(validColumn), Context.getDefault(), SSet.of(), SSet.of());
        final var validTable = new Table(new IdSimple(105), name, List.of(validColumn, invalidColumn1), Context.getDefault(), SSet.of(), SSet.of());

        final var transformation = new RemoveColumn();

        // --- Act
        final var newTableList = transformation.getCandidates(
                SSet.of(invalidTable1, invalidTable2, invalidTable3, invalidTable4, validTable)
        );

        // --- Assert
        Assertions.assertEquals(1, newTableList.size());
        Assertions.assertTrue(newTableList.contains(validTable));
    }
}