package scenarioCreator.generation.processing.transformations.constraintBased;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FunctionalDependencyRemover implements TableTransformation {

    private static boolean hasTableFds(Table table) {
        return !table.functionalDependencySet().isEmpty();
    }

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
        final var rte = new TransformationCouldNotBeExecutedException("Table is missing functional dependencies!");
        final var fdSet = FunctionalDependencyManager.minimalCover(table.functionalDependencySet());
        final var chosenFdSet = StreamExtensions.pickRandomOrThrow(fdSet.stream(), rte, random);
        final var newFdSet = StreamExtensions
                .replaceInStream(fdSet.stream(), chosenFdSet, Stream.of())
                .collect(Collectors.toCollection(TreeSet::new));
        final var newTableSet = SSet.of(table.withFunctionalDependencySet(newFdSet));
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO(80:20): tgds
        return new Pair<>(newTableSet, tgdList);
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(FunctionalDependencyRemover::hasTableFds)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}
