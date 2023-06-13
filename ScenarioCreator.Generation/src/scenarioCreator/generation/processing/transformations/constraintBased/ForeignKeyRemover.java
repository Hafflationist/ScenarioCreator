package scenarioCreator.generation.processing.transformations.constraintBased;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.constraintBased.base.ConstraintUtils;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ForeignKeyRemover implements SchemaTransformation {

    private static boolean isConstraintValid(
            ColumnConstraint constraint, Id purgeId
    ) {
        if (constraint instanceof ColumnConstraintForeignKeyInverse ccfki) {
            return !purgeId.equals(ccfki.foreignColumnId());
        }
        return true;
    }

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
    public Pair<Schema, List<TupleGeneratingDependency>> transform(Schema schema, Random random) {
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
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        return new Pair<>(newSchema, tgdList);
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