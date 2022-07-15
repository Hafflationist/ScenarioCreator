package de.mrobohm.transformations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import de.mrobohm.utils.StreamExtensions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

class NullableToVerticalInheritanceTest {

    @Test
    void transformWithoutPrimaryKey() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), Set.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(1), Set.of())));
        var invalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(4), Set.of())));
        var invalidColumn4 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(3), Set.of())));
        var validColumn = new ColumnLeaf(new IdSimple(5), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), Set.of());

        var invalidTable = new Table(new IdSimple(10), name, List.of(invalidColumn1, invalidColumn2),
                Context.getDefault(), Set.of());
        var targetTable = new Table(new IdSimple(14), name,
                List.of(invalidColumn3, invalidColumn4, validColumn),
                Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, targetTable);
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet));
        var idGenerator = StructuralTestingUtils.getIdGenerator(100);
        var transformation = new NullableToVerticalInheritance();

        // --- Act
        var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        var newTableList = newTableSet.stream().toList();
        var newTable1 = newTableList.get(0);
        var newTable2 = newTableList.get(1);
        Assertions.assertNotEquals(newTable1, newTable2);
        Assertions.assertEquals(
                targetTable.columnList().size() + 2, // 2 new surrogate keys
                newTable1.columnList().size() + newTable2.columnList().size()
        );
        Assertions.assertNotEquals(targetTable, newTable1);
        Assertions.assertNotEquals(targetTable, newTable2);
        var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toSet());
        var newSchema = new Schema(new IdSimple(-100), name, Context.getDefault(), fullNewTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transformWithPrimaryKey() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var primaryKeyColumn1 = new ColumnLeaf(new IdSimple(-1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        var primaryKeyColumn2 = new ColumnLeaf(new IdSimple(0), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        var invalidColumn0 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(3), Set.of())));
        var invalidColumn1 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(3), Set.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(1), Set.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(2), Set.of())));
        var validColumn = new ColumnLeaf(new IdSimple(4), name,
                dataType.withIsNullable(true), ColumnContext.getDefault(), Set.of());

        var invalidTable1 = new Table(new IdSimple(10), name,
                List.of(invalidColumn1), Context.getDefault(), Set.of());
        var invalidTable2 = new Table(new IdSimple(11), name,
                List.of(invalidColumn0, invalidColumn2), Context.getDefault(), Set.of());
        var targetTable = new Table(new IdSimple(14), name,
                List.of(primaryKeyColumn1, primaryKeyColumn2, validColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable1, invalidTable2, targetTable);
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet));
        var idGenerator = StructuralTestingUtils.getIdGenerator(100);
        var transformation = new NullableToVerticalInheritance();

        // --- Act
        var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        var newTableList = newTableSet.stream().toList();
        var newTable1 = newTableList.get(0);
        var newTable2 = newTableList.get(1);
        Assertions.assertEquals(5,newTable1.columnList().size() + newTable2.columnList().size());
        Assertions.assertNotEquals(targetTable, newTable1);
        Assertions.assertNotEquals(targetTable, newTable2);
        var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toSet());
        var newSchema = new Schema(new IdSimple(-100), name, Context.getDefault(), fullNewTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), Set.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(7), Set.of())));
        var validColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true), ColumnContext.getDefault(), Set.of());

        var invalidTable1 = new Table(new IdSimple(10), name,
                List.of(invalidColumn1), Context.getDefault(), Set.of());
        var invalidTable2 = new Table(new IdSimple(11), name,
                List.of(invalidColumn1, invalidColumn2), Context.getDefault(), Set.of());
        var validTable = new Table(new IdSimple(14), name,
                List.of(invalidColumn1, invalidColumn2, validColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable1, invalidTable2, validTable);
        var transformation = new NullableToVerticalInheritance();

        // --- Act
        var candidates = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertTrue(candidates.contains(validTable));
        Assertions.assertFalse(candidates.contains(invalidTable1));
        Assertions.assertFalse(candidates.contains(invalidTable2));
    }
}