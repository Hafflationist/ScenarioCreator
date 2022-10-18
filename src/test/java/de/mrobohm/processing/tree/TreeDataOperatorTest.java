package de.mrobohm.processing.tree;

import de.mrobohm.processing.tree.generic.TreeDataOperator;
import de.mrobohm.processing.tree.generic.TreeLeaf;
import de.mrobohm.processing.tree.generic.TreeNode;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TreeDataOperatorTest {

    @Test
    void replaceTreeEntityDeeplyNested() {
        // --- Arrange
        final var oldTe = new TreeLeaf<>(88);
        final var newTe = new TreeLeaf<>(23);
        final var oldTree = new TreeNode<>(0, SSet.of(
                new TreeNode<>(1, SSet.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                new TreeNode<>(2, SSet.of(
                        new TreeLeaf<>(21),
                        new TreeLeaf<>(22),
                        oldTe,
                        new TreeLeaf<>(24),
                        new TreeLeaf<>(25)
                )),
                new TreeNode<>(3, SSet.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));
        final var expectedTree = new TreeNode<>(0, SSet.of(
                new TreeNode<>(1, SSet.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                new TreeNode<>(2, SSet.of(
                        new TreeLeaf<>(21),
                        new TreeLeaf<>(22),
                        newTe,
                        new TreeLeaf<>(24),
                        new TreeLeaf<>(25)
                )),
                new TreeNode<>(3, SSet.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));

        // --- Act
        final var resultTree = TreeDataOperator.replaceTreeEntity(oldTree, oldTe, newTe);

        // --- Assert
        Assertions.assertEquals(expectedTree, resultTree);
    }

    @Test
    void replaceTreeEntityNested() {
        // --- Arrange
        final var oldTe = new TreeLeaf<>(88);
        final var newTe = new TreeLeaf<>(23);
        final var oldTree = new TreeNode<>(0, SSet.of(
                new TreeNode<>(1, SSet.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                oldTe,
                new TreeNode<>(3, SSet.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));
        final var expectedTree = new TreeNode<>(0, SSet.of(
                new TreeNode<>(1, SSet.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                newTe,
                new TreeNode<>(3, SSet.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));

        // --- Act
        final var resultTree = TreeDataOperator.replaceTreeEntity(oldTree, oldTe, newTe);

        // --- Assert
        Assertions.assertEquals(expectedTree, resultTree);
    }

    @Test
    void replaceTreeEntitySurface() {
        // --- Arrange
        final var oldTe = new TreeLeaf<>(88);
        final var newTe = new TreeLeaf<>(23);

        // --- Act
        final var resultTree = TreeDataOperator.replaceTreeEntity(oldTe, oldTe, newTe);

        // --- Assert
        Assertions.assertEquals(newTe, resultTree);
    }
}