package de.mrobohm.processing.tree;

import java.util.Set;

public sealed interface TreeEntity<TContent> permits TreeLeaf, TreeNode {
    TContent content();

    default TreeNode<TContent> withChildren(Set<TreeEntity<TContent>> newChildSet) {
        return new TreeNode<>(content(), newChildSet);
    }
}
