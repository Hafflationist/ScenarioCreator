package de.mrobohm.operations.structural;

import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.GroupColumnLeafsToNodeBase;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GroupColumnLeafsToNode implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet,
                                Function<Integer, int[]> idGenerator, Random random) {
        if (!(hasTableGroupableColumns(table))) {
            throw new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        }

        var groupableColumnList = GroupColumnLeafsToNodeBase.findGroupableColumns(table.columnList(), random);
        var newIds = idGenerator.apply(1);
        var newColumn = GroupColumnLeafsToNodeBase.createNewColumnNode(newIds[0], groupableColumnList, random);

        var newColumnList = StreamExtensions.replaceInStream(
                table.columnList().stream(),
                groupableColumnList.stream(),
                newColumn).toList();

        return Set.of(table.withColumnList(newColumnList));
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream().filter(this::hasTableGroupableColumns).collect(Collectors.toSet());
    }

    private boolean hasTableGroupableColumns(Table table) {
        return table.columnList().stream().anyMatch(GroupColumnLeafsToNodeBase::areConstraintsFine);
    }
}