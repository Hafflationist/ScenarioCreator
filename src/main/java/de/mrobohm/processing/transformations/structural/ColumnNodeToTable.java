package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.structural.base.NewTableBase;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ColumnNodeToTable implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return true;
    }

    @Override
    @NotNull
    public SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a node as column!");

        final var columnNodeStream = table.columnList().stream()
                .filter(c -> c instanceof ColumnNode);
        final var column = StreamExtensions.pickRandomOrThrow(columnNodeStream, exception, random);
        if (!(column instanceof ColumnNode node)) {
            throw new RuntimeException("Should never happen");
        }

        final var newIdArray = idGenerator.apply(3);
        final var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2]);
        final var newTable = NewTableBase.createNewTable(table, column.name(), node.columnList(), newIds, true);
        final var modifiedTable = NewTableBase.createModifiedTable(table, column, newIds, true);
        return SSet.of(newTable, modifiedTable);
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream().filter(this::hasColumnNode).collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean hasColumnNode(Table table) {
        return table.columnList().stream().anyMatch(c -> c instanceof ColumnNode);
    }
}