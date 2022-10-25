package de.mrobohm.processing.transformations.constraintBased;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.SchemaTransformation;
import de.mrobohm.processing.transformations.constraintBased.base.ConstraintUtils;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ForeignKeyRemover implements SchemaTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        final var rte = new TransformationCouldNotBeExecutedException(
                "No foreign key constraint found! Expected a column with a foreign key constraint!"
        );
        final var pair = chooseColumn(
                schema,
                columnList -> columnList.stream().filter(this::hasForeignKeyConstraint).toList(),
                random,
                rte
        );
        final var chosenTable = pair.first();
        final var chosenColumn = pair.second();
        final var newConstraintSet = chosenColumn.constraintSet().stream()
                .filter(c -> !(c instanceof ColumnConstraintForeignKey))
                .collect(Collectors.toCollection(TreeSet::new));

        final var newColumn = switch (chosenColumn) {
            case ColumnCollection c -> c.withConstraintSet(newConstraintSet);
            case ColumnLeaf c -> c.withConstraintSet(newConstraintSet);
            case ColumnNode c -> c.withConstraintSet(newConstraintSet);
        };
        final var newColumnList = chosenTable.columnList().stream()
                .map(column -> (column.equals(chosenColumn) ? newColumn : column))
                .toList();
        final var newTable = chosenTable.withColumnList(newColumnList);
        final var newTableSet = schema.tableSet().stream()
                .map(t -> (t.equals(chosenTable) ? newTable : t))
                .collect(Collectors.toCollection(TreeSet::new));
        final var newSchema = removeForeignKeyInverse(schema.withTableSet(newTableSet), chosenColumn.id());
        return newSchema;
    }

    private Pair<Table, Column> chooseColumn(
            Schema schema, Function<List<Column>, List<Column>> getCandidates, Random random, RuntimeException rte
    ) {
        assert schema.tableSet().size() > 0;

        final var candidateStream = schema.tableSet()
                .stream()
                .flatMap(t -> getCandidates.apply(t.columnList()).stream().map(column -> new Pair<>(t, column)));
        return StreamExtensions
                .pickRandomOrThrow(candidateStream, rte, random);
    }

    public Schema removeForeignKeyInverse(Schema schema, Id purgeId) {
        final var newTableSet = schema.tableSet().stream()
                .map(t -> ConstraintUtils.replaceConstraints(
                        t,
                        c -> isConstraintValid(c, purgeId) ? Stream.of(c) : Stream.of())
                )
                .collect(Collectors.toCollection(TreeSet::new));
        return schema.withTableSet(newTableSet);
    }

    private static boolean isConstraintValid(
            ColumnConstraint constraint, Id purgeId
    ) {
        if (constraint instanceof  ColumnConstraintForeignKeyInverse ccfki) {
            return purgeId != ccfki.foreignColumnId();
        }
        return true;
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .anyMatch(this::hasForeignKeyConstraint);
    }

    private boolean hasForeignKeyConstraint(Column column) {
        return column.containsConstraint(ColumnConstraintForeignKey.class);
    }
}