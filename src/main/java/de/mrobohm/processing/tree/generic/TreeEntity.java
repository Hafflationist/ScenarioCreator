package de.mrobohm.processing.tree.generic;

import org.jetbrains.annotations.NotNull;

import java.util.SortedSet;

public sealed interface TreeEntity<TContent> extends Comparable<TreeEntity> permits TreeLeaf, TreeNode {
    TContent content();

    default TreeNode<TContent> withChildren(SortedSet<TreeEntity<TContent>> newChildSet) {
        return new TreeNode<>(content(), newChildSet);
    }

    @Override
    default int compareTo(@NotNull TreeEntity te) {
        return this.toString().compareTo(te.toString());
    }
}
