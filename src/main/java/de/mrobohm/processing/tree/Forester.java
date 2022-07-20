package de.mrobohm.processing.tree;

import de.mrobohm.data.Schema;
import de.mrobohm.processing.transformations.SingleTransformationExecuter;
import de.mrobohm.processing.transformations.Transformation;
import de.mrobohm.processing.transformations.TransformationCollection;
import de.mrobohm.processing.transformations.exceptions.NoColumnFoundException;
import de.mrobohm.processing.transformations.exceptions.NoTableFoundException;
import de.mrobohm.utils.StreamExtensions;

import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Forester {

    private final SingleTransformationExecuter _singleTransformationExecuter;

    private final TransformationCollection _transformationCollection;

    private Forester(
            SingleTransformationExecuter singleTransformationExecuter,
            TransformationCollection transformationCollection
    ) {
        _singleTransformationExecuter = singleTransformationExecuter;
        _transformationCollection = transformationCollection;
    }

    public Set<Schema> createScenario(Schema rootSchema) {
        var tree = new TreeLeaf<>(rootSchema);
        // Hier wird die Stelle sein, an der alles zusammen läuft
        // Diese Methode definiert quasi die Anforderung des gesamten Programms.
        throw new RuntimeException("implement me!");
    }

    public TreeEntity<Schema> step(TreeEntity<Schema> te, TreeTargetDefinition ttd, Random random) {
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

    private Set<TreeEntity<Schema>> getAllTreeEntitySet(TreeEntity<Schema> te) {
        return switch (te) {
            case TreeLeaf<Schema> tl -> Set.of((TreeEntity<Schema>) tl);
            case TreeNode<Schema> tn -> {
                var children = tn.childSet().stream().flatMap(tnc -> getAllTreeEntitySet(tnc).stream());
                yield Stream.concat(Stream.of(tn), children).collect(Collectors.toSet());
            }
        };
    }

    private TreeEntity<Schema> chooseTreeEntityToExtend(TreeEntity<Schema> te, TreeTargetDefinition ttd, Random random) {
        var possibilitySet = getAllTreeEntitySet(te);
        var rte = new RuntimeException("This cannot happen. Probably there is a bug in <getAllTreeEntitySet>!");
        return StreamExtensions.pickRandomOrThrow(possibilitySet.stream(), rte, random);
    }

    private TreeEntity<Schema> extendTreeEntity(TreeEntity<Schema> te, Set<Transformation> transformationSet, Random random) {
        var newChild = createNewChild(te, transformationSet, random);
        var oldChildSet = switch (te) {
            case TreeNode<Schema> tn -> tn.childSet();
            case TreeLeaf ignore -> Set.<TreeEntity<Schema>>of();
        };
        var newChildSet = StreamExtensions
                .prepend(oldChildSet.stream(), newChild)
                .collect(Collectors.toSet());
        return te.withChildren(newChildSet);
    }

    private Set<Transformation> getChosenTransformations(
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
            Set<Transformation> transformationSet,
            Random random
    ) {
        return createNewChildInner(te, transformationSet, random, 10, 0);
    }


    private TreeLeaf<Schema> createNewChildInner(
            TreeEntity<Schema> te,
            Set<Transformation> transformationSet,
            Random random,
            int max,
            int acc
    ) {
        if (acc >= max) throw new RuntimeException("No suitable transformation could be found and performed!");
        var rte = new RuntimeException("Not enough transformations given!");
        var chosenTransformation = StreamExtensions.pickRandomOrThrow(
                transformationSet.stream(), rte, random
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