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
import scenarioCreator.generation.processing.tree.TgdChainElement;
import scenarioCreator.utils.StreamExtensions;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Forester implements IForester {

    private final SingleTransformationExecutor _singleTransformationExecutor;

    private final TransformationCollection _transformationCollection;

    private final DistanceMeasures _measures;

    private final DistanceDefinition _validDefinition;
    private final DistanceDefinition _targetDefinition;
    private final int _numberOfSteps;

    public Forester(
            SingleTransformationExecutor singleTransformationExecutor,
            TransformationCollection transformationCollection,
            DistanceMeasures measures,
            DistanceDefinition validDefinition,
            DistanceDefinition targetDefinition,
            int numberOfSteps
    ) {
        _singleTransformationExecutor = singleTransformationExecutor;
        _transformationCollection = transformationCollection;
        _measures = measures;
        _validDefinition = validDefinition;
        _targetDefinition = targetDefinition;
        _numberOfSteps = numberOfSteps;
    }

    public SchemaAsResult createNext(
            SchemaInForest rootSchema,
            TreeGenerationDefinition tgd,
            SortedSet<Schema> oldSchemaSet,
            int newChildren,
            Random random,
            boolean debug
    ) {
        final var tree = new TreeLeaf<>(rootSchema);
        final var sifSet = Stream
                .iterate((TreeEntity<SchemaInForest>) tree, t -> step(
                        t, tgd, oldSchemaSet, newChildren, random, debug
                ))
                .limit(_numberOfSteps)
                .flatMap(te -> TreeDataOperator.getAllTreeEntityList(te).stream())
                .map(TreeEntity::content)
                .filter(sif -> !sif.equals(rootSchema))
                .collect(Collectors.toCollection(TreeSet::new));
        return chooseBestChild(sifSet, random);
    }

    private TreeEntity<SchemaInForest> step(
            TreeEntity<SchemaInForest> oldTree,
            TreeGenerationDefinition tgd,
            SortedSet<Schema> oldSchemaSet,
            int newChildren,
            Random random,
            boolean debug
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
                chosenTe, transformationSet, oldSchemaSet, newChildren, random, debug
        );
        return TreeDataOperator.replaceTreeEntity(oldTree, chosenTe, newTe);
    }

    private TreeEntity<SchemaInForest> chooseTreeEntityToExtend(
            TreeEntity<SchemaInForest> te, Random random
    ) {
        // Falls ein Knoten bereits das Ziel erfüllt, soll ein zufälliger Knoten erweitert werden
        final var possibilityList = TreeDataOperator.getAllTreeEntityList(te);
        final var targetNodeExists = possibilityList.parallelStream()
                .map(TreeEntity::content)
                .map(SchemaInForest::distanceList)
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

    private List<TgdChainElement> sifToChain(SchemaInForest sif) {
        if (sif.predecessorOpt().isEmpty()) {
            return List.of();
        }
        final var preSif = sif.predecessorOpt().get();
        final var preTgdChain = sifToChain(preSif);
        final var tgdChainElement = new TgdChainElement(preSif.schema(), sif.tgdList(), sif.schema());
        return Stream.concat(
                preTgdChain.stream(),
                Stream.of(tgdChainElement)
        ).toList();
    }

    private SchemaAsResult chooseBestChild(SortedSet<SchemaInForest> sifSet, Random random) {
        final var targetNodeStream = sifSet.stream()
                .filter(sif -> DistanceHelper.isValid(
                        sif.distanceList(), _targetDefinition, DistanceHelper.AggregationMethod.AVERAGE
                ));
        final var targetNodeOpt = StreamExtensions.tryPickRandom(targetNodeStream, random);
        if (targetNodeOpt.isPresent()) {
            return new SchemaAsResult(
                    targetNodeOpt.get().schema(),
                    sifToChain(targetNodeOpt.get()),
                    targetNodeOpt.get().distanceList(),
                    targetNodeOpt.get().executedTransformationList(),
                    true,
                    true
            );
        }
        final var validNodeStream = sifSet.stream()
                .filter(sif -> DistanceHelper.isValid(
                        sif.distanceList(), _validDefinition, DistanceHelper.AggregationMethod.CONJUNCTION
                ));
        final var validNodeOpt = StreamExtensions.tryPickRandom(validNodeStream, random);
        if (validNodeOpt.isPresent()) {
            return new SchemaAsResult(
                    validNodeOpt.get().schema(),
                    sifToChain(validNodeOpt.get()),
                    validNodeOpt.get().distanceList(),
                    validNodeOpt.get().executedTransformationList(),
                    false,
                    true
            );
        }
        final var rte = new RuntimeException("No children generated!");
        final var randomNode = StreamExtensions.pickRandomOrThrow(sifSet.stream(), rte, random);
        return new SchemaAsResult(
                randomNode.schema(),
                sifToChain(randomNode),
                randomNode.distanceList(),
                randomNode.executedTransformationList(),
                false,
                false
        );
    }

    private TreeEntity<SchemaInForest> extendTreeEntity(
            TreeEntity<SchemaInForest> te,
            SortedSet<Transformation> transformationSet,
            SortedSet<Schema> oldSchemaSet,
            int newChildrenNum,
            Random random,
            boolean debug
    ) {
        final var newChildren = Stream
                .iterate(0, o -> 0)
                .map(o -> createNewChild(te, transformationSet, oldSchemaSet, random, debug))
                .limit(newChildrenNum);
        final var oldChildList = switch (te) {
            case TreeNode<SchemaInForest> tn -> tn.childList();
            case TreeLeaf ignore -> List.<TreeEntity<SchemaInForest>>of();
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

    private TreeLeaf<SchemaInForest> createNewChild(
            TreeEntity<SchemaInForest> te,
            SortedSet<Transformation> transformationSet,
            SortedSet<Schema> oldSchemaSet,
            Random random,
            boolean debug
    ) {
        return createNewChildInner(te, transformationSet, oldSchemaSet, random, 10, 0, debug);
    }

    private TreeLeaf<SchemaInForest> createNewChildInner(
            TreeEntity<SchemaInForest> te,
            SortedSet<Transformation> transformationSet,
            SortedSet<Schema> oldSchemaSet,
            Random random,
            int max,
            int acc,
            boolean debug
    ) {
        if (acc >= max) throw new RuntimeException("No suitable transformation could be found and performed!");
        final var rte = new RuntimeException("Not enough transformations given!");
        final var schema = te.content().schema();
        final var validTransformationStream = transformationSet.stream()
                .filter(transformation -> SingleTransformationChecker.checkTransformation(schema, transformation));
        final var chosenTransformation = StreamExtensions.pickRandomOrThrow(
                validTransformationStream, rte, random
        );
        System.out.println("ENTKÄFERUNG: Forester: ChosenTransformation = " + chosenTransformation.toString());
        try {
            final var newSchemaWithTgds = _singleTransformationExecutor.executeTransformation(
                    schema, chosenTransformation, random, debug
            );
            final var newSchema = newSchemaWithTgds.first();
            final var newDistanceList = DistanceHelper.distanceList(newSchema, oldSchemaSet, _measures);
            final var newExecutedTransformationList = Stream.concat(
                    te.content().executedTransformationList().stream(),
                    Arrays.stream(chosenTransformation.toString().split("@")).limit(1)
            ).toList();
            final var newSchemaInForest = new SchemaInForest(
                    Optional.of(te.content()), newSchemaWithTgds.second(), newSchema, newDistanceList, newExecutedTransformationList
            );
            return new TreeLeaf<>(newSchemaInForest);
        } catch (NoTableFoundException | NoColumnFoundException e) {
            return createNewChildInner(te, transformationSet, oldSchemaSet, random, max, acc + 1, debug);
        }
    }
}
