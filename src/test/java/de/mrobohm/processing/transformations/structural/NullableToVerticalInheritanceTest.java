package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

class NullableToVerticalInheritanceTest {

    @Test
    void transformWithoutPrimaryKey() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1), SSet.of())));
        final var invalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(4), SSet.of())));
        final var invalidColumn4 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())));
        final var validColumn = new ColumnLeaf(new IdSimple(5), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());

        final var invalidTable = StructuralTestingUtils.createTable(
                10, List.of(invalidColumn1, invalidColumn2)
        );
        final var targetTable = StructuralTestingUtils.createTable(
                14, List.of(invalidColumn3, invalidColumn4, validColumn)
        );
        final var tableSet = SSet.of(invalidTable, targetTable);
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet));
        final var idGenerator = StructuralTestingUtils.getIdGenerator(600);
        final var transformation = new NullableToVerticalInheritance();

        // --- Act
        final var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        final var newTableList = newTableSet.stream().toList();
        final var newTable1 = newTableList.get(0);
        final var newTable2 = newTableList.get(1);
        Assertions.assertNotEquals(newTable1, newTable2);
        Assertions.assertEquals(
                targetTable.columnList().size() + 2, // 2 new surrogate keys
                newTable1.columnList().size() + newTable2.columnList().size()
        );
        Assertions.assertNotEquals(targetTable, newTable1);
        Assertions.assertNotEquals(targetTable, newTable2);
        final var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toCollection(TreeSet::new));
        final var newSchema = new Schema(new IdSimple(-100), name, Context.getDefault(), fullNewTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transformWithPrimaryKey() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var primaryKeyColumn1 = new ColumnLeaf(new IdSimple(-1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        final var primaryKeyColumn2 = new ColumnLeaf(new IdSimple(0), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        final var invalidColumn0 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())));
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(1), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var validColumn = new ColumnLeaf(new IdSimple(4), name,
                dataType.withIsNullable(true), ColumnContext.getDefault(), SSet.of());

        final var invalidTable1 = StructuralTestingUtils.createTable(
                10, List.of(invalidColumn1)
        );
        final var invalidTable2 = StructuralTestingUtils.createTable(
                11, List.of(invalidColumn0, invalidColumn2)
        );
        final var targetTable = StructuralTestingUtils.createTable(
                14, List.of(primaryKeyColumn1, primaryKeyColumn2, validColumn)
        );
        final var tableSet = SSet.of(invalidTable1, invalidTable2, targetTable);
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet));
        final var idGenerator = StructuralTestingUtils.getIdGenerator(100);
        final var transformation = new NullableToVerticalInheritance();

        // --- Act
        final var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        final var newTableList = newTableSet.stream().toList();
        final var newTable1 = newTableList.get(0);
        final var newTable2 = newTableList.get(1);
        Assertions.assertEquals(5, newTable1.columnList().size() + newTable2.columnList().size());
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
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7), SSet.of())));
        final var validColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true), ColumnContext.getDefault(), SSet.of());

        final var invalidTable1 = new Table(new IdSimple(10), name,
                List.of(invalidColumn1), Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable2 = new Table(new IdSimple(11), name,
                List.of(invalidColumn1, invalidColumn2), Context.getDefault(), SSet.of(), SSet.of());
        final var validTable = new Table(new IdSimple(14), name,
                List.of(invalidColumn1, invalidColumn2, validColumn), Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(invalidTable1, invalidTable2, validTable);
        final var transformation = new NullableToVerticalInheritance();

        // --- Act
        final var candidates = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertTrue(candidates.contains(validTable));
        Assertions.assertFalse(candidates.contains(invalidTable1));
        Assertions.assertFalse(candidates.contains(invalidTable2));
    }
}