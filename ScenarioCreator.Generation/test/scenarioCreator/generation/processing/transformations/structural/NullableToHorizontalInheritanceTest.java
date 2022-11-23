package scenarioCreator.generation.processing.transformations.structural;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdPart;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

class NullableToHorizontalInheritanceTest {

    @Test
    void transform() {
    }

    @Test
    void transformWithPrimaryKey() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var primaryKeyColumn1 = new ColumnLeaf(new IdSimple(31), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        final var primaryKeyColumn2 = new ColumnLeaf(new IdSimple(32), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        final var invalidColumn0 = new ColumnLeaf(new IdSimple(21), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22))));
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(11), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22))));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(22), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(invalidColumn0.id()),
                        new ColumnConstraintForeignKeyInverse(invalidColumn1.id())));
        final var validColumn = new ColumnLeaf(new IdSimple(33), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());

        final var invalidTable1 = StructuralTestingUtils.createTable(
                101, List.of(invalidColumn1)
        );
        final var invalidTable2 = StructuralTestingUtils.createTable(
                102, List.of(invalidColumn0, invalidColumn2)
        );
        final var targetTable = StructuralTestingUtils.createTable(
                103, List.of(primaryKeyColumn1, primaryKeyColumn2, validColumn)
        );
        final var tableSet = SSet.of(invalidTable1, invalidTable2, targetTable);
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet));
        final var idGenerator = StructuralTestingUtils.getIdGenerator(1200);
        final var transformation = new NullableToHorizontalInheritance();

        // --- Act
        final var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        final var newTableList = newTableSet.stream().toList();
        final var newTable1 = newTableList.get(0);
        final var newTable2 = newTableList.get(1);
        Assertions.assertEquals(targetTable.columnList().size(), Math.max(newTable1.columnList().size(), newTable2.columnList().size()));
        Assertions.assertEquals(targetTable.columnList().size() - 1, Math.min(newTable1.columnList().size(), newTable2.columnList().size()));
        Assertions.assertEquals(targetTable.columnList().size() - 1,
                newTable1.columnList().stream().filter(column -> column.id() instanceof IdPart).count());
        Assertions.assertEquals(targetTable.columnList().size() - 1,
                newTable2.columnList().stream().filter(column -> column.id() instanceof IdPart).count());
        Assertions.assertNotEquals(targetTable, newTable1);
        Assertions.assertNotEquals(targetTable, newTable2);
        final var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toCollection(TreeSet::new));
        final var newSchema = new Schema(new IdSimple(-100), name, Context.getDefault(), fullNewTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void getCandidates() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6))));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7))));
        final var validColumn1 = new ColumnLeaf(
                new IdSimple(4), name, dataType.withIsNullable(true), ColumnContext.getDefault(), SSet.of()
        );
        final var validColumn2 = new ColumnLeaf(
                new IdSimple(5), name, dataType.withIsNullable(true), ColumnContext.getDefault(), SSet.of()
        );

        final var invalidTable1 = new Table(new IdSimple(10), name, List.of(invalidColumn1),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable2 = new Table(new IdSimple(11), name, List.of(invalidColumn1, invalidColumn2),
                Context.getDefault(), SSet.of(), SSet.of());
        final var validTable = new Table(new IdSimple(14), name, List.of(validColumn1, validColumn2),
                Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(invalidTable1, invalidTable2, validTable);
        final var transformation = new NullableToHorizontalInheritance();

        // --- Act
        final var candidates = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertTrue(candidates.contains(validTable));
        Assertions.assertFalse(candidates.contains(invalidTable1));
        Assertions.assertFalse(candidates.contains(invalidTable2));
    }
}