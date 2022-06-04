package de.mrobohm.operations.structural;

import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.GroupColumnLeafsToNodeBase;
import de.mrobohm.operations.structural.base.NewTableBase;
import de.mrobohm.operations.structural.generator.IdentificationNumberGenerator;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ColumnLeafsToTable implements TableTransformation {
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

        var newColumnList = GroupColumnLeafsToNodeBase.findGroupableColumns(table.columnList(), random);
        var newName = GroupColumnLeafsToNodeBase.mergeNames(newColumnList, random);
        var newIdArray = IdentificationNumberGenerator.generate(otherTableSet, 4);
        var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2], newIdArray[3]);
        var newTable = NewTableBase.createNewTable(newName, newColumnList, newIds, true);
        var modifiedTable = NewTableBase.createModifiedTable(table, newName, newColumnList, newIds, true);
        return Set.of(newTable, modifiedTable);
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