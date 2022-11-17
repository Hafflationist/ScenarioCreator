package scenarioCreator.generation.processing.tree;

import scenarioCreator.data.Schema;
import scenarioCreator.generation.processing.transformations.SingleTransformationChecker;
import scenarioCreator.generation.processing.transformations.SingleTransformationExecutor;
import scenarioCreator.generation.processing.transformations.Transformation;
import scenarioCreator.generation.processing.transformations.TransformationCollection;
import scenarioCreator.generation.processing.transformations.exceptions.NoColumnFoundException;
import scenarioCreator.generation.processing.transformations.exceptions.NoTableFoundException;
import scenarioCreator.generation.processing.tree.generic.TreeDataOperator;
import scenarioCreator.generation.processing.tree.generic.TreeEntity;
import scenarioCreator.generation.processing.tree.generic.TreeLeaf;
import scenarioCreator.generation.processing.tree.generic.TreeNode;
import scenarioCreator.utils.StreamExtensions;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Forester implements IForester {

    private static final int NUMBER_OF_STEPS = 16;

    private final SingleTransformationExecutor _singleTransformationExecutor;

    private final TransformationCollection _transformationCollection;

    private final DistanceMeasures _measures;

    private final DistanceDefinition _validDefinition;
    private final DistanceDefinition _targetDefinition;

    public Forester(
            SingleTransformationExecutor singleTransformationExecutor,
            TransformationCollection transformationCollection,
            DistanceMeasures measures,
            DistanceDefinition validDefinition,
            DistanceDefinition targetDefinition
    ) {
        _singleTransformationExecutor = singleTransformationExecutor;
        _transformationCollection = transformationCollection;
        _measures = measures;
        _validDefinition = validDefinition;
        _targetDefinition = targetDefinition;
    }

    public SchemaWithAdditionalData createNext(
            SchemaWithAdditionalData rootSchema,
            TreeGenerationDefinition tgd,
            SortedSet<Schema> oldSchemaSet,
            int newChildren,
            Random random
    ) {
        final var tree = new TreeLeaf<>(rootSchema);
        final var swadSet = Stream
                .iterate((TreeEntity<SchemaWithAdditionalData>) tree, t -> step(t, tgd, oldSchemaSet, newChildren, random))
                .limit(NUMBER_OF_STEPS)
                .flatMap(te -> TreeDataOperator.getAllTreeEntityList(te).stream())
                .map(TreeEntity::content)
                .filter(swad -> !swad.equals(rootSchema))
                .collect(Collectors.toCollection(TreeSet::new));
        return chooseBestChild(swadSet, random);
    }

    private TreeEntity<SchemaWithAdditionalData> step(
            TreeEntity<SchemaWithAdditionalData> oldTree,
            TreeGenerationDefinition tgd,
            SortedSet<Schema> oldSchemaSet,
            int newChildren,
            Random random
    ) {
        final var chosenTe = chooseTreeEntityToExtend(oldTree, random);
        final var transformationSet = getChosenTransformations(
                _transformationCollection,
                tgd.keepForeignKeyIntegrity(),
                tgd.shouldConserveAllRecords(),
                tgd.shouldStayNormalized(),
                tgd.conservesFlatRelations()
        );
        final var newTe = extendTreeEntity(
                chosenTe, transformationSet, oldSchemaSet, newChildren, random
        );
        return TreeDataOperator.replaceTreeEntity(oldTree, chosenTe, newTe);
    }

    private TreeEntity<SchemaWithAdditionalData> chooseTreeEntityToExtend(
            TreeEntity<SchemaWithAdditionalData> te, Random random
    ) {
        // Falls ein Knoten bereits das Ziel erfüllt, soll ein zufälliger Knoten erweitert werden
        final var possibilityList = TreeDataOperator.getAllTreeEntityList(te);
        final var targetNodeExists = possibilityList.parallelStream()
                .map(TreeEntity::content)
                .map(SchemaWithAdditionalData::distanceList)
                .allMatch(dl -> DistanceHelper.isValid(dl, _targetDefinition, DistanceHelper.AggregationMethod.AVERAGE));
        final var rte = new RuntimeException("This cannot happen. Probably there is a bug in <getAllTreeEntityList>!");
        if (targetNodeExists) {
            return StreamExtensions.pickRandomOrThrow(possibilityList.stream(), rte, random);
        }
        final var chosenNodeOpt = possibilityList.stream()
                .min(Comparator.comparing(node -> {
                    final var distAvg = DistanceHelper.avg(node.content().distanceList());
                    return _targetDefinition.diff(distAvg);
                }));
        if (chosenNodeOpt.isEmpty()) {
            throw rte;
        }
        return chosenNodeOpt.get();
    }

    private SchemaWithAdditionalData chooseBestChild(SortedSet<SchemaWithAdditionalData> swadSet, Random random) {
        final var targetNodeStream = swadSet.stream()
                .filter(swad -> DistanceHelper.isValid(
                        swad.distanceList(), _targetDefinition, DistanceHelper.AggregationMethod.AVERAGE
                ));
        final var targetNodeOpt = StreamExtensions.tryPickRandom(targetNodeStream, random);
        if (targetNodeOpt.isPresent()) {
            return targetNodeOpt.get();
        }
        final var validNodeStream = swadSet.stream()
                .filter(swad -> DistanceHelper.isValid(
                        swad.distanceList(), _validDefinition, DistanceHelper.AggregationMethod.CONJUNCTION
                ));
        final var validNodeOpt = StreamExtensions.tryPickRandom(validNodeStream, random);
        if (validNodeOpt.isPresent()) {
            return validNodeOpt.get();
        }
        final var rte = new RuntimeException("No children generated!");
        return StreamExtensions.pickRandomOrThrow(swadSet.stream(), rte, random);
    }

    private TreeEntity<SchemaWithAdditionalData> extendTreeEntity(
            TreeEntity<SchemaWithAdditionalData> te,
            SortedSet<Transformation> transformationSet,
            SortedSet<Schema> oldSchemaSet,
            int newChildrenNum,
            Random random
    ) {
        final var newChildren = Stream
                .iterate(0, o -> 0)
                .map(o -> createNewChild(te, transformationSet, oldSchemaSet, random))
                .limit(newChildrenNum);
        final var oldChildList = switch (te) {
            case TreeNode<SchemaWithAdditionalData> tn -> tn.childList();
            case TreeLeaf ignore -> List.<TreeEntity<SchemaWithAdditionalData>>of();
        };
        final var newChildList = Stream
                .concat(oldChildList.stream(), newChildren)
                .toList();
        return te.withChildren(newChildList);
    }

    private SortedSet<Transformation> getChosenTransformations(
            TransformationCollection transformationCollection,
            boolean keepForeignKeyIntegrity,
            boolean shouldConserveAllRecords,
            boolean shouldStayNormalized,
            boolean conservesFlatRelations
    ) {
        return transformationCollection.getAllTransformationSet(
                keepForeignKeyIntegrity,
                shouldConserveAllRecords,
                shouldStayNormalized,
                conservesFlatRelations
        );
    }

    private TreeLeaf<SchemaWithAdditionalData> createNewChild(
            TreeEntity<SchemaWithAdditionalData> te,
            SortedSet<Transformation> transformationSet,
            SortedSet<Schema> oldSchemaSet,
            Random random
    ) {
        return createNewChildInner(te, transformationSet, oldSchemaSet, random, 10, 0);
    }

    private TreeLeaf<SchemaWithAdditionalData> createNewChildInner(
            TreeEntity<SchemaWithAdditionalData> te,
            SortedSet<Transformation> transformationSet,
            SortedSet<Schema> oldSchemaSet,
            Random random,
            int max,
            int acc
    ) {
        if (acc >= max) throw new RuntimeException("No suitable transformation could be found and performed!");
        final var rte = new RuntimeException("Not enough transformations given!");
        final var schema = te.content().schema();
        final var validTransformationStream = transformationSet.stream()
                .filter(transformation -> SingleTransformationChecker.checkTransformation(schema, transformation));
        final var chosenTransformation = StreamExtensions.pickRandomOrThrow(
                validTransformationStream, rte, random
        );
        try {
            final var newSchema = _singleTransformationExecutor.executeTransformation(
                    schema, chosenTransformation, random
            );
            final var newDistanceList = DistanceHelper.distanceList(newSchema, oldSchemaSet, _measures);
            final var newSchemaWithAdditionalData = new SchemaWithAdditionalData(newSchema, newDistanceList);
            return new TreeLeaf<>(newSchemaWithAdditionalData);
        } catch (NoTableFoundException | NoColumnFoundException e) {
            return createNewChildInner(te, transformationSet, oldSchemaSet, random, max, acc + 1);
        }
    }
}