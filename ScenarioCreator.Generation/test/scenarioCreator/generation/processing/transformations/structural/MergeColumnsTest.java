package scenarioCreator.generation.processing.transformations.structural;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdMerge;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.generation.processing.transformations.structural.MergeColumns;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;

class MergeColumnsTest {

    Table getTable(Id id, SortedSet<Table> tableSet) {
        final var tableOpt = tableSet.stream().filter(t -> t.id().equals(id)).findFirst();
        assert tableOpt.isPresent();
        return tableOpt.get();
    }

    @Test
    void transformTrue() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var validDataType = new DataType(DataTypeEnum.NVARCHAR, false);
        final var semivalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())));
        final var semivalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())));
        final var semivalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(1), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(20))));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(21))));
        final var invalidColumn3 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(22))));
        final var validColumn1 = new ColumnLeaf(new IdSimple(7), name, validDataType,
                ColumnContext.getDefault(), SSet.of());
        final var validColumn2 = validColumn1.withId(new IdSimple(8));

        final var invalidTable = StructuralTestingUtils.createTable(
                10, List.of(invalidColumn1, semivalidColumn1)
        );
        final var semivalidTable = StructuralTestingUtils.createTable(
                11, List.of(invalidColumn2, semivalidColumn2, semivalidColumn3)
        );
        final var validTable = StructuralTestingUtils.createTable(
                14, List.of(invalidColumn3, validColumn1, validColumn2)
        );
        final var tableSet = SSet.of(invalidTable, validTable, semivalidTable);
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new MergeColumns(true);

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        final var newTableSet = newSchema.tableSet();
        Assertions.assertEquals(3, newTableSet.size());
        final var newInvalidTable = getTable(invalidTable.id(), newTableSet);
        Assertions.assertEquals(invalidTable, newInvalidTable);
        final var newSemivalidTable = getTable(semivalidTable.id(), newTableSet);
        Assertions.assertEquals(semivalidTable, newSemivalidTable);
        final var newValidTable = getTable(validTable.id(), newTableSet);
        Assertions.assertNotEquals(validTable, newValidTable);
        Assertions.assertEquals(validTable.name(), newValidTable.name());
        Assertions.assertNotEquals(validTable.columnList(), newValidTable.columnList());
        Assertions.assertEquals(validTable.columnList().size() - 1, newValidTable.columnList().size());
        Assertions.assertTrue(newValidTable.columnList().stream().anyMatch(column -> column.id() instanceof IdMerge));
        Assertions.assertEquals(validTable.context(), newValidTable.context());
        Assertions.assertEquals(validTable.tableConstraintSet(), newValidTable.tableConstraintSet());
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transformFalse() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var semivalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())));
        final var semivalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())));
        final var semivalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(1), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(20))));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(21))));

        final var invalidTable = StructuralTestingUtils.createTable(
                10, List.of(invalidColumn1, semivalidColumn1)
        );
        final var semivalidTable = StructuralTestingUtils.createTable(
                11, List.of(invalidColumn2, semivalidColumn2, semivalidColumn3)
        );
        final var tableSet = SSet.of(invalidTable, semivalidTable);
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new MergeColumns(false);

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        final var newTableSet = newSchema.tableSet();
        Assertions.assertEquals(2, newTableSet.size());
        final var newInvalidTable = getTable(invalidTable.id(), newTableSet);
        Assertions.assertEquals(invalidTable.name(), newInvalidTable.name());
        Assertions.assertEquals(invalidTable.context(), newInvalidTable.context());
        Assertions.assertEquals(invalidTable.tableConstraintSet(), newInvalidTable.tableConstraintSet());
        final var newSemivalidTable = getTable(semivalidTable.id(), newTableSet);
        Assertions.assertNotEquals(semivalidTable, newSemivalidTable);
        Assertions.assertEquals(semivalidTable.name(), newSemivalidTable.name());
        Assertions.assertNotEquals(semivalidTable.columnList(), newSemivalidTable.columnList());
        Assertions.assertEquals(semivalidTable.columnList().size() - 1, newSemivalidTable.columnList().size());
        Assertions.assertEquals(semivalidTable.context(), newSemivalidTable.context());
        Assertions.assertEquals(semivalidTable.tableConstraintSet(), newSemivalidTable.tableConstraintSet());
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transformNdDeletion() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var nd = new NumericalDistribution(1.0, Map.of(1, 0.2, 2, 0.9));
        final var validColumn1 = new ColumnLeaf(new IdSimple(7), name, dataType,
                ColumnContext.getDefaultWithNd(nd), SSet.of());
        final var validColumn2 = validColumn1.withId(new IdSimple(8));

        final var table = StructuralTestingUtils.createTable(
                14, List.of(validColumn1, validColumn2)
        );
        final var tableSet = SSet.of(table);
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new MergeColumns(false);

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        final var newTableSet = newSchema.tableSet();
        Assertions.assertEquals(1, newTableSet.size());
        final var newTable = getTable(table.id(), newTableSet);
        Assertions.assertEquals(1, newTable.columnList().size());
        final var newColumn = newTable.columnList().get(0);
        assert newColumn instanceof ColumnLeaf;
        Assertions.assertEquals(NumericalDistribution.getDefault(), ((ColumnLeaf) newColumn).context().numericalDistribution());
        IntegrityChecker.assertValidSchema(newSchema);

    }
        @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void isExecutable(boolean keepForeignKeyIntegrity) {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var semivalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        final var semivalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7), SSet.of())));
        final var invalidColumn = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(8))));
        final var validColumn1 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var validColumn2 = validColumn1.withId(new IdSimple(5));

        final var invalidTable = new Table(new IdSimple(10), name,
                List.of(invalidColumn, semivalidColumn1), Context.getDefault(), SSet.of(), SSet.of());
        final var semivalidTable1 = new Table(new IdSimple(11), name,
                List.of(invalidColumn, semivalidColumn1, semivalidColumn2), Context.getDefault(), SSet.of(), SSet.of());
        final var semivalidTable2 = new Table(new IdSimple(12), name,
                List.of(invalidColumn, semivalidColumn1, validColumn1), Context.getDefault(), SSet.of(), SSet.of());
        final var semivalidTable3 = new Table(new IdSimple(13), name,
                List.of(invalidColumn, validColumn1, semivalidColumn2), Context.getDefault(), SSet.of(), SSet.of());
        final var validTable = new Table(new IdSimple(14), name,
                List.of(invalidColumn, validColumn1, validColumn2), Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(invalidTable, semivalidTable1, semivalidTable2, semivalidTable3, validTable);
        final var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        final var transformation = new MergeColumns(keepForeignKeyIntegrity);

        // --- Act
        final var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }
}