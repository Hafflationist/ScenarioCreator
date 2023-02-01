package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.constraint.*;
import scenarioCreator.data.column.constraint.numerical.CheckConjunction;
import scenarioCreator.data.column.constraint.numerical.CheckDisjunction;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdMerge;
import scenarioCreator.data.identification.MergeOrSplitType;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.constraintBased.base.CheckNumericalManager;
import scenarioCreator.generation.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.structural.base.IdTranslation;
import scenarioCreator.generation.processing.transformations.structural.base.IngestionBase;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HorizontalInheritanceToNullable implements SchemaTransformation {


    private final int _primaryKeyCountThreshold;

    private final double _jaccardThreshold;

    public HorizontalInheritanceToNullable(int primaryKeyCountThreshold, double jaccardThreshold) {
        _primaryKeyCountThreshold = primaryKeyCountThreshold;
        _jaccardThreshold = jaccardThreshold;
    }

    public HorizontalInheritanceToNullable(double jaccardThreshold) {
        _primaryKeyCountThreshold = 2;  // just trust every confirmed non-surrogate primary key correspondence
        _jaccardThreshold = jaccardThreshold;
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
    public Schema transform(Schema schema, Random random) {
        final var exception = new TransformationCouldNotBeExecutedException("Given schema did not include horizontal inheritance");
        if (!isExecutable(schema)) {
            throw exception;
        }

        final var ip = findDerivingTable(schema.tableSet(), random);
        final var derivationIntegrationResult = integrateDerivation(ip);
        final var oldTableStream = Stream.of(ip.base(), ip.derivation());

        final var newTableSet = StreamExtensions
                .replaceInStream(schema.tableSet().stream(), oldTableStream, derivationIntegrationResult.newTable())
                .collect(Collectors.toCollection(TreeSet::new));

        return IdTranslation.translateConstraints(
                schema.withTableSet(newTableSet),
                derivationIntegrationResult.idTranslationMap(),
                Set.of()
        );
    }

    private InheritancePair findDerivingTable(SortedSet<Table> tableSet, Random random) {
        final var ipStream = tableSet.stream()
                .flatMap(t1 -> tableSet.stream().map(t2 -> new Pair<>(t1, t2)))
                .map(pair -> new InheritancePair(pair.first(), pair.second()))
                .filter(this::isDeriving);
        final var ipOptional = StreamExtensions.tryPickRandom(ipStream, random);
        assert ipOptional.isPresent() : "This should be handled before!";
        return ipOptional.get();
    }

    private DerivationIntegrationResult integrateDerivation(InheritancePair ip) {
        final var columnPairStream = ip.derivation().columnList().stream()
                .map(derivationColumn ->
                        new Pair<>(derivationColumn, ip.base().columnList().stream()
                                .filter(baseColumn -> equalsColumns(derivationColumn, baseColumn))
                                .findFirst()));
        final var columnStreamShouldAdd = StreamExtensions
                .partition(
                        columnPairStream,
                        pair -> pair.second().isEmpty()
                );

        final var additionalColumnStream = columnStreamShouldAdd.yes()
                .map(pair -> switch (pair.first()) {
                    case ColumnLeaf leaf -> leaf.withDataType(leaf.dataType().withIsNullable(true));
                    case ColumnNode node -> node.withIsNullable(true);
                    case ColumnCollection col -> col.withIsNullable(true);
                });
        final var mergeablePairList = columnStreamShouldAdd.no().toList();
        final var mergedColumnStream = mergeablePairList.stream()
                .map(pair -> {
                    assert pair.second().isPresent();
                    final var newId = new IdMerge(pair.first().id(), pair.second().get().id(), MergeOrSplitType.Xor);
                    final var newConstraintSet = mergeXor(
                            pair.first().constraintSet(), pair.second().get().constraintSet()
                    );
                    return (Column) switch (pair.first()) {
                        case ColumnLeaf leaf -> {
                            final var newNd = (pair.second().get() instanceof ColumnLeaf)
                                    ?
                                    CheckNumericalManager.merge(
                                            leaf.context().numericalDistribution(),
                                            ((ColumnLeaf) pair.second().get()).context().numericalDistribution()
                                    )
                                    : leaf.context().numericalDistribution();

                            yield leaf
                                    .withId(newId)
                                    .withContext(leaf.context().withNumericalDistribution(newNd))
                                    .withConstraintSet(newConstraintSet);
                        }
                        case ColumnNode node -> node.withId(newId).withConstraintSet(newConstraintSet);
                        case ColumnCollection col -> col.withId(newId).withConstraintSet(newConstraintSet);
                    };
                });
        final var newColumnList = Stream.concat(mergedColumnStream, additionalColumnStream).toList();
        final var newFdSet = FunctionalDependencyManager.getValidFdSet(
                ip.base.functionalDependencySet(), newColumnList
        );
        final var newTable = ip.base()
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFdSet)
                .withId(new IdMerge(ip.base.id(), ip.derivation.id(), MergeOrSplitType.Xor));
        // komplett falsch:
        final var idTranslationMap = mergeablePairList.stream()
                .filter(pair -> pair.second().isPresent())
                .flatMap(pair -> {
                    final var newId = (Id) new IdMerge(pair.first().id(), pair.second().get().id(), MergeOrSplitType.Xor);
                    final var newIdSet = SSet.of(newId);
                    return Stream.of(
                            new Pair<>(pair.first().id(), newIdSet),
                            new Pair<>(pair.second().get().id(), newIdSet)
                    );
                })
                .collect(Collectors.toMap(Pair::first, Pair::second));
        return new DerivationIntegrationResult(newTable, idTranslationMap);
    }

    private SortedSet<ColumnConstraint> mergeXor(
            SortedSet<ColumnConstraint> constraintSet1, SortedSet<ColumnConstraint> constraintSet2
    ) {
        final var partition = StreamExtensions.partition(
                constraintSet1.stream(),
                c -> c instanceof ColumnConstraintCheckNumerical
        );
        final var conjunction1 = new CheckConjunction(
                partition.yes()
                        .map(c -> (ColumnConstraintCheckNumerical) c)
                        .map(ColumnConstraintCheckNumerical::checkExpression)
                        .collect(Collectors.toCollection(TreeSet::new))
        );
        final var conjunction2 = new CheckConjunction(
                constraintSet2.stream()
                        .filter(c -> c instanceof ColumnConstraintCheckNumerical)
                        .map(c -> (ColumnConstraintCheckNumerical) c)
                        .map(ColumnConstraintCheckNumerical::checkExpression)
                        .collect(Collectors.toCollection(TreeSet::new))
        );
        final var newCheckExpression = new CheckDisjunction(SSet.of(conjunction1, conjunction2));
        final var newNumericalConstraint = new ColumnConstraintCheckNumerical(newCheckExpression);
        // TODO: maybe (don't!) simplify checkExpression
        return SSet.prepend(newNumericalConstraint, partition.no().collect(Collectors.toCollection(TreeSet::new)));
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return schema.tableSet().stream().anyMatch(t1 -> schema.tableSet().stream()
                .anyMatch(t2 -> isDeriving(new InheritancePair(t1, t2))));
    }

    private boolean isDeriving(InheritancePair ip) {
        final var relationshipCount = IngestionBase.getRelationshipCount(ip.base(), ip.derivation());
        if (relationshipCount > 0 || ip.base().equals(ip.derivation())) {
            return false;
        }

        final var basePartition = StreamExtensions
                .partition(
                        ip.base().columnList().stream(),
                        column -> column.containsConstraint(ColumnConstraintPrimaryKey.class)
                );

        final var derivingPartition = StreamExtensions
                .partition(
                        ip.derivation().columnList().stream(),
                        column -> column.containsConstraint(ColumnConstraintPrimaryKey.class)
                );

        // Check whether both primary key column lists are equal
        final var basePrimaryKeyColumnList = basePartition.yes().toList();
        final var derivingPrimaryKeyColumnList = derivingPartition.yes().toList();
        if (!equalsColumnList(basePrimaryKeyColumnList, derivingPrimaryKeyColumnList)) {
            return false;
        } else if (basePrimaryKeyColumnList.size() >= _primaryKeyCountThreshold) {
            return true;
        }

        // Check whether there are enough equal columns
        final var jaccardIndex = jaccard(basePartition.no().toList(), derivingPartition.no().toList());
        return jaccardIndex >= _jaccardThreshold && isSubset(ip.derivation().columnList(), ip.base().columnList());
    }

    private double jaccard(List<Column> columnListA, List<Column> columnListB) {
        final var intersection = columnListA.stream()
                .distinct()
                .filter(c1 -> columnListB.stream().anyMatch(c2 -> equalsColumns(c1, c2)))
                .count();
        final var union = Stream
                .concat(
                        columnListA.stream(),
                        columnListB.stream().filter(c2 -> columnListA.stream().noneMatch(c1 -> equalsColumns(c1, c2))))
                .count();
        return (double) intersection / union;
    }

    private boolean isSubset(List<Column> superset, List<Column> subset) {
        return subset.stream().allMatch(sub -> superset.stream().anyMatch(sup -> equalsColumns(sub, sup)));
    }

    private boolean equalsColumnList(List<Column> columnListA, List<Column> columnListB) {
        if (columnListA.size() != columnListB.size()) {
            return false;
        }
        return StreamExtensions
                .zip(columnListA.stream(), columnListB.stream(), this::equalsColumns)
                .allMatch(x -> x);
    }

    private boolean equalsColumns(Column c, Column d) {
        if (c instanceof ColumnLeaf cLeaf && d instanceof ColumnLeaf dLeaf) {
            return c.name().equals(d.name())
                    && cLeaf.dataType().equals(dLeaf.dataType())
                    && equalsConstraintSets(c.constraintSet(), d.constraintSet());
        }
        if (c instanceof ColumnNode cNode && d instanceof ColumnNode dNode) {
            return c.name().equals(d.name())
                    && c.isNullable() == d.isNullable()
                    && equalsConstraintSets(c.constraintSet(), d.constraintSet())
                    && equalsColumnList(cNode.columnList(), dNode.columnList());
        }
        if (c instanceof ColumnCollection cCol && d instanceof ColumnCollection dCol) {
            return c.name().equals(d.name())
                    && c.isNullable() == d.isNullable()
                    && equalsConstraintSets(c.constraintSet(), d.constraintSet())
                    && equalsColumnList(cCol.columnList(), dCol.columnList());
        }
        return false;
    }

    private boolean isSubsetConstraintSets(
            SortedSet<ColumnConstraint> constraintSetSuper, SortedSet<ColumnConstraint> constraintSetSub
    ) {
        return constraintSetSub.stream().allMatch(ca -> constraintSetSuper.stream().anyMatch(cb -> switch (ca) {
            case ColumnConstraintPrimaryKey ignore -> cb instanceof ColumnConstraintPrimaryKey;
            case ColumnConstraintUnique ignore -> cb instanceof ColumnConstraintUnique;
            case ColumnConstraintForeignKey ccfka -> cb instanceof ColumnConstraintForeignKey ccfkb
                    && ccfka.foreignColumnId().equals(ccfkb.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfkia -> cb instanceof ColumnConstraintForeignKeyInverse ccfkib
                    && ccfkia.foreignColumnId().equals(ccfkib.foreignColumnId());
            // the following constraints can be merged in the methode named mergeXor
            case ColumnConstraintCheckNumerical ignore -> true;
            case ColumnConstraintCheckRegex ignore -> true;
        }));
    }

    private boolean equalsConstraintSets(
            SortedSet<ColumnConstraint> constraintSetA, SortedSet<ColumnConstraint> constraintSetB
    ) {
        return isSubsetConstraintSets(constraintSetA, constraintSetB)
                && isSubsetConstraintSets(constraintSetB, constraintSetA);
    }

    private record DerivationIntegrationResult(Table newTable, Map<Id, SortedSet<Id>> idTranslationMap) {
    }

    private record InheritancePair(Table derivation, Table base) {
    }
}