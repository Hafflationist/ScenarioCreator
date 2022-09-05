package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.structural.base.GroupingColumnsBase;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GroupColumnLeafsToNode implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        if (!(GroupingColumnsBase.containsGroupableColumns(table.columnList()))) {
            throw new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        }

        final var groupableColumnList = GroupingColumnsBase.findGroupableColumns(table.columnList(), random);
        final var newIds = idGenerator.apply(1);
        final var newColumn = GroupingColumnsBase.createNewColumnNode(newIds[0], groupableColumnList, random);

        final var newColumnList = StreamExtensions.replaceInStream(
                table.columnList().stream(),
                groupableColumnList.stream(),
                newColumn).toList();

        return SSet.of(table.withColumnList(newColumnList));
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> GroupingColumnsBase.containsGroupableColumns(t.columnList()))
                .collect(Collectors.toCollection(TreeSet::new));
    }
}