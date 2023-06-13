package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.CharBase;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AddTypoToTableName implements TableTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var newName = CharBase.introduceTypo(table.name(), random);
        final var newTableSet = SSet.of(table.withName(newName));
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        return new Pair<>(newTableSet, tgdList);
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> t.name().rawString(LinguisticUtils::merge).length() > 0)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}