package de.mrobohm.processing.tree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class TreeDataOperatorTest {

    @Test
    void replaceTreeEntityDeeplyNested() {
        // --- Arrange
        var oldTe = new TreeLeaf<>(88);
        var newTe = new TreeLeaf<>(23);
        var oldTree = new TreeNode<>(0, Set.of(
                new TreeNode<>(1, Set.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                new TreeNode<>(2, Set.of(
                        new TreeLeaf<>(21),
                        new TreeLeaf<>(22),
                        oldTe,
                        new TreeLeaf<>(24),
                        new TreeLeaf<>(25)
                )),
                new TreeNode<>(3, Set.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));
        var expectedTree = new TreeNode<>(0, Set.of(
                new TreeNode<>(1, Set.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                new TreeNode<>(2, Set.of(
                        new TreeLeaf<>(21),
                        new TreeLeaf<>(22),
                        newTe,
                        new TreeLeaf<>(24),
                        new TreeLeaf<>(25)
                )),
                new TreeNode<>(3, Set.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));

        // --- Act
        var resultTree = TreeDataOperator.replaceTreeEntity(oldTree, oldTe, newTe);

        // --- Assert
        Assertions.assertEquals(expectedTree, resultTree);
    }

    @Test
    void replaceTreeEntityNested() {
        // --- Arrange
        var oldTe = new TreeLeaf<>(88);
        var newTe = new TreeLeaf<>(23);
        var oldTree = new TreeNode<>(0, Set.of(
                new TreeNode<>(1, Set.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                oldTe,
                new TreeNode<>(3, Set.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));
        var expectedTree = new TreeNode<>(0, Set.of(
                new TreeNode<>(1, Set.of(
                        new TreeLeaf<>(11),
                        new TreeLeaf<>(12),
                        new TreeLeaf<>(13),
                        new TreeLeaf<>(14),
                        new TreeLeaf<>(15)
                )),
                newTe,
                new TreeNode<>(3, Set.of(
                        new TreeLeaf<>(31),
                        new TreeLeaf<>(32),
                        new TreeLeaf<>(33),
                        new TreeLeaf<>(34),
                        new TreeLeaf<>(35)
                ))
        ));

        // --- Act
        var resultTree = TreeDataOperator.replaceTreeEntity(oldTree, oldTe, newTe);

        // --- Assert
        Assertions.assertEquals(expectedTree, resultTree);
    }

    @Test
    void replaceTreeEntitySurface() {
        // --- Arrange
        var oldTe = new TreeLeaf<>(88);
        var newTe = new TreeLeaf<>(23);

        // --- Act
        var resultTree = TreeDataOperator.replaceTreeEntity(oldTe, oldTe, newTe);

        // --- Assert
        Assertions.assertEquals(newTe, resultTree);
    }
}