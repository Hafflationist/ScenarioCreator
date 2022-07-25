package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.structural.base.GroupingColumnsBase;
import de.mrobohm.processing.transformations.structural.base.NewTableBase;
import de.mrobohm.utils.SSet;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

// equivalent to vertical split
public class ColumnLeafsToTable implements TableTransformation {
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
        if (!(GroupingColumnsBase.containsGroupableColumns(table.columnList()))) {
            throw new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        }

        var newColumnList = GroupingColumnsBase.findGroupableColumns(table.columnList(), random);
        assert newColumnList.size() > 0;
        var newName = GroupingColumnsBase.mergeNames(newColumnList, random);
        var newIdArray = idGenerator.apply(3);
        var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2]);
        var newTable = NewTableBase.createNewTable(table, newName, newColumnList, newIds, true);
        var modifiedTable = NewTableBase.createModifiedTable(table, newName, newColumnList, newIds, true);
        return SSet.of(newTable, modifiedTable);
    }


    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> GroupingColumnsBase.containsGroupableColumns(t.columnList()))
                .collect(Collectors.toCollection(TreeSet::new));
    }
}