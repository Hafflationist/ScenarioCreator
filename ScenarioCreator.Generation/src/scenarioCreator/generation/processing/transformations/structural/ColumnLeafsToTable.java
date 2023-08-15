package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.ReducedRelation;
import scenarioCreator.data.tgds.RelationConstraint;
import scenarioCreator.data.tgds.RelationConstraintEquality;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.structural.base.GroupingColumnsBase;
import scenarioCreator.generation.processing.transformations.structural.base.NewTableBase;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.List;
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
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        if (!(GroupingColumnsBase.containsGroupableColumns(table.columnList()))) {
            throw new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        }

        final var newColumnList = GroupingColumnsBase.findGroupableColumns(table.columnList(), random);
        assert newColumnList.size() > 0;
        final var newName = GroupingColumnsBase.mergeNames(newColumnList, random);
        final var newIdArray = idGenerator.apply(3);
        final var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2]);
        final var newTable = NewTableBase.createNewTable(table, newName, newColumnList, newIds, true);
        final var modifiedTable = NewTableBase.createModifiedTable(table, newName, newColumnList, newIds, true);
        final var newTableSet = SSet.of(newTable, modifiedTable);
        final List<TupleGeneratingDependency> tgdList = tgds(table, modifiedTable, newTable, newIds);
        return new Pair<>(newTableSet, tgdList);
    }

    private List<TupleGeneratingDependency> tgds(Table sourceTable, Table modifiedTable, Table newTable, NewTableBase.NewIds newIds) {
       final var sourceRelation = ReducedRelation.fromTable(sourceTable);
       final var modifiedRelation = ReducedRelation.fromTable(modifiedTable);
       final var newRelation = ReducedRelation.fromTable(newTable);
       // TODO(80:20): Im folgenden fehlt die Einzigartigkeit des Primärschlüssels
       final var constraints = List.of(
               (RelationConstraint) new RelationConstraintEquality(newIds.sourceColumn(), newIds.targetColumn())
       );
       return List.of(
           new TupleGeneratingDependency(List.of(sourceRelation), List.of(modifiedRelation, newRelation), constraints)
       );
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> GroupingColumnsBase.containsGroupableColumns(t.columnList()))
                .collect(Collectors.toCollection(TreeSet::new));
    }
}
