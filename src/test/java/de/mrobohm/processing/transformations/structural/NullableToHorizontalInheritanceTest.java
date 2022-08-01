package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdPart;
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

class NullableToHorizontalInheritanceTest {

    @Test
    void transform() {
    }

    @Test
    void transformWithPrimaryKey() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var primaryKeyColumn1 = new ColumnLeaf(new IdSimple(31), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        var primaryKeyColumn2 = new ColumnLeaf(new IdSimple(32), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        var invalidColumn0 = new ColumnLeaf(new IdSimple(21), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22), SSet.of())));
        var invalidColumn1 = new ColumnLeaf(new IdSimple(11), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22), SSet.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(22), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(invalidColumn0.id(), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(invalidColumn1.id(), SSet.of())));
        var validColumn = new ColumnLeaf(new IdSimple(33), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());

        var invalidTable1 = StructuralTestingUtils.createTable(
                101, List.of(invalidColumn1)
        );
        var invalidTable2 = StructuralTestingUtils.createTable(
                102, List.of(invalidColumn0, invalidColumn2)
        );
        var targetTable = StructuralTestingUtils.createTable(
                103, List.of(primaryKeyColumn1, primaryKeyColumn2, validColumn)
        );
        var tableSet = SSet.of(invalidTable1, invalidTable2, targetTable);
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet));
        var idGenerator = StructuralTestingUtils.getIdGenerator(1200);
        var transformation = new NullableToHorizontalInheritance();

        // --- Act
        var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        var newTableList = newTableSet.stream().toList();
        var newTable1 = newTableList.get(0);
        var newTable2 = newTableList.get(1);
        Assertions.assertEquals(targetTable.columnList().size(), Math.max(newTable1.columnList().size(), newTable2.columnList().size()));
        Assertions.assertEquals(targetTable.columnList().size() - 1, Math.min(newTable1.columnList().size(), newTable2.columnList().size()));
        Assertions.assertEquals(targetTable.columnList().size() - 1,
                newTable1.columnList().stream().filter(column -> column.id() instanceof IdPart).count());
        Assertions.assertEquals(targetTable.columnList().size() - 1,
                newTable2.columnList().stream().filter(column -> column.id() instanceof IdPart).count());
        Assertions.assertNotEquals(targetTable, newTable1);
        Assertions.assertNotEquals(targetTable, newTable2);
        var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toCollection(TreeSet::new));
        var newSchema = new Schema(new IdSimple(-100), name, Context.getDefault(), fullNewTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7), SSet.of())));
        var validColumn1 = new ColumnLeaf(
                new IdSimple(4), name, dataType.withIsNullable(true), ColumnContext.getDefault(), SSet.of()
        );
        var validColumn2 = new ColumnLeaf(
                new IdSimple(5), name, dataType.withIsNullable(true), ColumnContext.getDefault(), SSet.of()
        );

        var invalidTable1 = new Table(new IdSimple(10), name, List.of(invalidColumn1),
                Context.getDefault(), SSet.of(), SSet.of());
        var invalidTable2 = new Table(new IdSimple(11), name, List.of(invalidColumn1, invalidColumn2),
                Context.getDefault(), SSet.of(), SSet.of());
        var validTable = new Table(new IdSimple(14), name, List.of(validColumn1, validColumn2),
                Context.getDefault(), SSet.of(), SSet.of());
        var tableSet = SSet.of(invalidTable1, invalidTable2, validTable);
        var transformation = new NullableToHorizontalInheritance();

        // --- Act
        var candidates = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertTrue(candidates.contains(validTable));
        Assertions.assertFalse(candidates.contains(invalidTable1));
        Assertions.assertFalse(candidates.contains(invalidTable2));
    }
}