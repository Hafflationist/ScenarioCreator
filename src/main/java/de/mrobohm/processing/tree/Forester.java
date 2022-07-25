package de.mrobohm.processing.tree;

import de.mrobohm.data.Schema;
import de.mrobohm.processing.transformations.SingleTransformationChecker;
import de.mrobohm.processing.transformations.SingleTransformationExecuter;
import de.mrobohm.processing.transformations.Transformation;
import de.mrobohm.processing.transformations.TransformationCollection;
import de.mrobohm.processing.transformations.exceptions.NoColumnFoundException;
import de.mrobohm.processing.transformations.exceptions.NoTableFoundException;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Forester {

    private final SingleTransformationExecuter _singleTransformationExecuter;

    private final TransformationCollection _transformationCollection;

    public Forester(
            SingleTransformationExecuter singleTransformationExecuter,
            TransformationCollection transformationCollection
    ) {
        _singleTransformationExecuter = singleTransformationExecuter;
        _transformationCollection = transformationCollection;
    }

    public SortedSet<Schema> createScenario(Schema rootSchema, TreeTargetDefinition ttd, Random random) {
        var tree = new TreeLeaf<>(rootSchema);
        return Stream
                .iterate((TreeEntity<Schema>)tree, t -> step(t, ttd, random))
                .limit(5)
                .flatMap(te -> getAllTreeEntitySet(te).stream())
                .map(TreeEntity::content)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private TreeEntity<Schema> step(TreeEntity<Schema> te, TreeTargetDefinition ttd, Random random) {
        var chosenTe = chooseTreeEntityToExtend(te, ttd, random);
        var transformationSet = getChosenTransformations(
                _transformationCollection,
                ttd.keepForeignKeyIntegrity(),
                ttd.shouldConserveAllRecords(),
                ttd.shouldStayNormalized(),
                ttd.conservesFlatRelations()
        );
        var newTe = extendTreeEntity(te, transformationSet, random);
        return TreeDataOperator.replaceTreeEntity(te, chosenTe, newTe);
    }

    private SortedSet<TreeEntity<Schema>> getAllTreeEntitySet(TreeEntity<Schema> te) {
        return switch (te) {
            case TreeLeaf<Schema> tl -> SSet.of((TreeEntity<Schema>) tl);
            case TreeNode<Schema> tn -> {
                var children = tn.childSet().parallelStream()
                        .flatMap(tnc -> getAllTreeEntitySet(tnc).stream());
                yield Stream.concat(Stream.of(tn), children).collect(Collectors.toCollection(TreeSet::new));
            }
        };
    }

    private TreeEntity<Schema> chooseTreeEntityToExtend(TreeEntity<Schema> te, TreeTargetDefinition ttd, Random random) {
        var possibilitySet = getAllTreeEntitySet(te);
        var rte = new RuntimeException("This cannot happen. Probably there is a bug in <getAllTreeEntitySet>!");
        return StreamExtensions.pickRandomOrThrow(possibilitySet.stream(), rte, random);
    }

    private TreeEntity<Schema> extendTreeEntity(TreeEntity<Schema> te, SortedSet<Transformation> transformationSet, Random random) {
        var newChild = createNewChild(te, transformationSet, random);
        var oldChildSet = switch (te) {
            case TreeNode<Schema> tn -> tn.childSet();
            case TreeLeaf ignore -> SSet.<TreeEntity<Schema>>of();
        };
        var newChildSet = StreamExtensions
                .prepend(oldChildSet.stream(), newChild)
                .collect(Collectors.toCollection(TreeSet::new));
        return te.withChildren(newChildSet);
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

    private TreeLeaf<Schema> createNewChild(
            TreeEntity<Schema> te,
            SortedSet<Transformation> transformationSet,
            Random random
    ) {
        return createNewChildInner(te, transformationSet, random, 10, 0);
    }


    private TreeLeaf<Schema> createNewChildInner(
            TreeEntity<Schema> te,
            SortedSet<Transformation> transformationSet,
            Random random,
            int max,
            int acc
    ) {
        if (acc >= max) throw new RuntimeException("No suitable transformation could be found and performed!");
        var rte = new RuntimeException("Not enough transformations given!");
        // TODO: check which transformations can be applied!
        var schema = te.content();
        var validTransformationStream = transformationSet.stream()
                .filter(transformation -> SingleTransformationChecker.checkTransformation(schema, transformation));
        var chosenTransformation = StreamExtensions.pickRandomOrThrow(
                validTransformationStream, rte, random
        );
        try {
            var newSchema = _singleTransformationExecuter.executeTransformation(
                    te.content(), chosenTransformation, random
            );
            return new TreeLeaf<>(newSchema);
        } catch (NoTableFoundException | NoColumnFoundException e) {
            return createNewChildInner(te, transformationSet, random, max, acc + 1);
        }
    }
}