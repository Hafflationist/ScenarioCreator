package de.mrobohm.operations;


import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.exceptions.NoColumnFoundException;
import de.mrobohm.operations.exceptions.NoTableFoundException;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformationExecuter {

    @Contract(pure = true)
    @NotNull
    public Schema executeTransformationSchema(Schema schema, SchemaTransformation transformation) {
        return transformation.transform(schema);
    }


    @Contract(pure = true)
    @NotNull
    private Table chooseTable(Set<Table> tableSet) throws NoTableFoundException {
        var tableStream = tableSet.stream();
        return StreamExtensions.pickRandomOrThrow(tableStream, new NoTableFoundException());
    }

    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationTable(Schema schema, Table targetTable, Set<Table> newTableSet) {
        assert schema.tableSet().contains(targetTable);

        var filteredTableStream = schema.tableSet()
                .stream()
                .filter(table -> !table.equals(targetTable));
        var newTableStream = Stream.concat(filteredTableStream, newTableSet.stream());
        return schema.withTables(newTableStream.collect(Collectors.toSet()));
    }


    @Contract(pure = true)
    @NotNull
    public Schema executeTransformationTable(Schema schema, TableTransformation transformation)
            throws NoTableFoundException {
        var targetTable = chooseTable(transformation.getCandidates(schema.tableSet()));
        var newTableSet = transformation.transform(targetTable, schema.tableSet());
        return executeTransformationTable(schema, targetTable, newTableSet);
    }


    @Contract(pure = true)
    @NotNull
    public Schema executeTransformationColumn(Schema schema, ColumnTransformation transformation)
            throws NoTableFoundException, NoColumnFoundException {
        assert schema != null;

        var target = chooseColumn(schema, transformation::getCandidates);
        var targetTable = target.first();
        var targetColumn = target.second();
        var newPartialColumnStream = transformation.transform(targetColumn).stream();

        var oldColumnStream = targetTable.columnList().stream();
        var newColumnList = StreamExtensions
                .replaceInStream(oldColumnStream, targetColumn, newPartialColumnStream)
                .toList();
        var newTableSet = Collections.singleton(targetTable.withColumnList(newColumnList));

        return executeTransformationTable(schema, target.first(), newTableSet);
    }


    @Contract(pure = true)
    @NotNull
    private Pair<Table, Column> chooseColumn(Schema schema, Function<List<Column>, List<Column>> getCandidates)
            throws NoColumnFoundException, NoTableFoundException {
        assert schema.tableSet().size() > 0;

        var candidateTableSet = schema.tableSet()
                .stream()
                .filter(table -> getCandidates.apply(table.columnList()).size() > 0)
                .collect(Collectors.toSet());

        var targetTable = chooseTable(candidateTableSet);
        var columnStream = targetTable.columnList().stream();
        var targetColumn = StreamExtensions.pickRandomOrThrow(columnStream, new NoColumnFoundException());
        return new Pair<>(targetTable, targetColumn);
    }
}
