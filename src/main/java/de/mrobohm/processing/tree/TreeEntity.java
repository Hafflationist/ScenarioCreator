package de.mrobohm.processing.tree;

import de.mrobohm.data.Schema;

import java.util.Set;

public sealed interface TreeEntity permits TreeLeaf, TreeNode {
    Schema schema();

    default TreeNode withChildren(Set<TreeEntity> newChildSet) {
        return new TreeNode(schema(), newChildSet);
    }
}
