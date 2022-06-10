package de.mrobohm.operations.structural;

import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.NewTableBase;
import de.mrobohm.operations.structural.generator.IdentificationNumberGenerator;
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
    public Set<Table> transform(Table table, Set<Table> otherTableSet,
                                Function<Integer, int[]> idGenerator, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a collection as column!");

        var columnCollectionStream = table.columnList().stream()
                .filter(c -> c instanceof ColumnCollection);
        var column = StreamExtensions.pickRandomOrThrow(columnCollectionStream, exception, random);
        if (!(column instanceof ColumnCollection collection)) {
            throw new RuntimeException("Should never happen");
        }

        var newIdArray = IdentificationNumberGenerator.generate(otherTableSet, 4);
        var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2], newIdArray[3]);
        var newTable = NewTableBase.createNewTable(column.name(), collection.columnList(), newIds, false);
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