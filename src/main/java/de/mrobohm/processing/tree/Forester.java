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

public class Forester {

    private final SingleTransformationExecuter _singleTransformationExecuter;

    private Forester(SingleTransformationExecuter singleTransformationExecuter) {
        _singleTransformationExecuter = singleTransformationExecuter;
    }

    public Set<Schema> createScenario(Schema rootSchema) {
        var tree = new TreeLeaf(rootSchema);
        // Hier wird die Stelle sein, an der alles zusammen läuft
        // Diese Methode definiert quasie die Anforderung des gesamten Programms.
        throw new RuntimeException("implement me!");
    }

    private TreeEntity extendTreeEntity(TreeEntity te, Set<Transformation> transformationSet, Random random) {
        var newChild = createNewChild(te, transformationSet, random);
        var oldChildSet = switch (te) {
            case TreeNode tn -> tn.childSet();
            case TreeLeaf ignore -> Set.<TreeEntity>of();
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

    private TreeLeaf createNewChild(
            TreeEntity te,
            Set<Transformation> transformationSet,
            Random random
    ) {
        return createNewChildInner(te, transformationSet, random, 10, 0);
    }


    private TreeLeaf createNewChildInner(
            TreeEntity te,
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
                    te.schema(), chosenTransformation, random
            );
            return new TreeLeaf(newSchema);
        } catch (NoTableFoundException | NoColumnFoundException e) {
            return createNewChildInner(te, transformationSet, random, max, acc + 1);
        }
    }
}
