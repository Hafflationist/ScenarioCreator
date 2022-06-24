package de.mrobohm.operations.structural;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.GroupingColumnsBase;
import de.mrobohm.operations.structural.base.NewTableBase;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

// equivalent to vertical split
public class ColumnLeafsToTable implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet,
                                Function<Integer, Id[]> idGenerator, Random random) {
        if (!(GroupingColumnsBase.containsGroupableColumns(table.columnList()))) {
            throw new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        }

        var newColumnList = GroupingColumnsBase.findGroupableColumns(table.columnList(), random);
        assert newColumnList.size() > 0;
        var newName = GroupingColumnsBase.mergeNames(newColumnList, random);
        var newIdArray = idGenerator.apply(4);
        var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2], newIdArray[3]);
        var newTable = NewTableBase.createNewTable(newName, newColumnList, newIds, true);
        var modifiedTable = NewTableBase.createModifiedTable(table, newName, newColumnList, newIds, true);
        return Set.of(newTable, modifiedTable);
    }


    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> GroupingColumnsBase.containsGroupableColumns(t.columnList()))
                .collect(Collectors.toSet());
    }
}