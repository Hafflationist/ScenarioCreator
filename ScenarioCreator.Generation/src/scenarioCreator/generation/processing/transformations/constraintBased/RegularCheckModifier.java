package scenarioCreator.generation.processing.transformations.constraintBased;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.constraint.ColumnConstraintCheckRegex;
import scenarioCreator.data.column.constraint.regexy.*;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RegularCheckModifier implements ColumnTransformation {

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
    public Pair<List<Column>, List<TupleGeneratingDependency>> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        if (!hasCheckRegexConstraint(column)) {
            throw new TransformationCouldNotBeExecutedException("No check regular constraint found! Expected a column with a check regular constraint!");
        }

        final var newConstraintSet = column.constraintSet().stream()
                .map(c -> c instanceof ColumnConstraintCheckRegex cccr ? modify(cccr, random) : c)
                .collect(Collectors.toCollection(TreeSet::new));

        final var newColumnList = List.of((Column) switch (column) {
            case ColumnCollection c -> c.withConstraintSet(newConstraintSet);
            case ColumnLeaf c -> c.withConstraintSet(newConstraintSet);
            case ColumnNode c -> c.withConstraintSet(newConstraintSet);
        });
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        return new Pair<>(newColumnList, tgdList);
    }

    private ColumnConstraintCheckRegex modify(ColumnConstraintCheckRegex cccr, Random random) {
        final var regex = cccr.regularExpression();
        final var newRegex = amplify(this::oneLevelModify).apply(regex, random);
        if (regex.equals(newRegex)) return modify(cccr, random);
        return new ColumnConstraintCheckRegex(newRegex);
    }

    private BiFunction<RegularExpression, Random, RegularExpression> amplify(
            BiFunction<RegularExpression, Random, RegularExpression> oneLevelModifier
    ) {
        return (regex, random) -> {
            final var chosenNode = StreamExtensions.pickRandomOrThrow(
                    unfold(regex).stream(), new RuntimeException("Cannot happen!"), random
            );
            final var newNode = oneLevelModifier.apply(chosenNode, random);
            return replace(regex, chosenNode, newNode);
        };
    }
    
    private SortedSet<RegularExpression> unfold(RegularExpression regex) {
        final SortedSet<RegularExpression> nestedRegexSet = switch (regex) {
            case RegularConcatenation rc -> SSet.concat(unfold(rc.expression1()), unfold(rc.expression2()));
            case RegularKleene rk -> unfold(rk.expression());
            case RegularSum rs -> SSet.concat(unfold(rs.expression1()), unfold(rs.expression2()));
            default -> SSet.of();
        };
        return SSet.prepend(regex, nestedRegexSet);
    }

    private RegularExpression replace(RegularExpression tree, RegularExpression oldNode, RegularExpression newNode) {
        if (tree.equals(oldNode)) return newNode;
        return switch (tree) {
            case RegularConcatenation rc -> new RegularConcatenation(
                    replace(rc.expression1(), oldNode, newNode),
                    replace(rc.expression2(), oldNode, newNode)
            );
            case RegularKleene rk -> new RegularKleene(
                    replace(rk.expression(), oldNode, newNode)
            );
            case RegularSum rs -> new RegularSum(
                    replace(rs.expression1(), oldNode, newNode),
                    replace(rs.expression2(), oldNode, newNode)
            );
            default -> tree;
        };
    }

    private RegularExpression oneLevelModify(RegularExpression regex, Random random) {
        if (random.nextBoolean()){
           return addContainer(regex, random);
        }
        return switch (regex) {
            case RegularConcatenation rc -> random.nextBoolean() ? rc.expression1() : rc.expression2();
            case RegularKleene rk -> rk.expression();
            case RegularTerminal ignore ->  {
                if(3 == random.nextInt(8)) yield new RegularWildcard();
                yield newTerminal(random);
            }
            case RegularSum rs -> random.nextBoolean() ? rs.expression1() : rs.expression2();
            case RegularWildcard ignore -> newTerminal(random);
        };
    }

    private RegularTerminal newTerminal(Random random) {
        final var newChar = StreamExtensions.pickRandomOrThrow(
                RegularExpression.inputAlphabet(),
                new RuntimeException("Input alphabet should not be empty!"),
                random
        );
        return new RegularTerminal(newChar);
    }

    private RegularExpression addContainer(RegularExpression regex, Random random) {
        return switch (random.nextInt(5)){
            case 0 -> new RegularKleene(regex);
            case 1 -> new RegularConcatenation(regex, newTerminal(random));
            case 2 -> new RegularConcatenation(newTerminal(random), regex);
            case 3 -> new RegularSum(regex, newTerminal(random));
            default -> addContainer(regex, random);
        };
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::hasCheckRegexConstraint).toList();
    }

    private boolean hasCheckRegexConstraint(Column column) {
        return column.containsConstraint(ColumnConstraintCheckRegex.class);
    }
}