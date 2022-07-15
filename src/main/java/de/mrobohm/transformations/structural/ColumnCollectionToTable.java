package de.mrobohm.transformations.structural;

import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.transformations.TableTransformation;
import de.mrobohm.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.transformations.structural.base.NewTableBase;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ColumnCollectionToTable implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a collection as column!");

        var columnCollectionList = table.columnList().stream()
                .filter(c -> c instanceof ColumnCollection)
                .toList();
        var column = StreamExtensions.pickRandomOrThrow(columnCollectionList.stream(), exception, random);
        if (!(column instanceof ColumnCollection collection)) {
            throw new RuntimeException("Should never happen");
        }

        var newIdArray = idGenerator.apply(3);
        var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2]);
        var newTable = NewTableBase.createNewTable(
                table, column.name(), collection.columnList(), newIds, false
        );
        var modifiedTable = NewTableBase.createModifiedTable(table, column, newIds, false);
        return Set.of(newTable, modifiedTable);
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream().filter(this::hasColumnCollection).collect(Collectors.toSet());
    }

    private boolean hasColumnCollection(Table table) {
        return table.columnList().stream().anyMatch(c -> c instanceof ColumnCollection);
    }
}