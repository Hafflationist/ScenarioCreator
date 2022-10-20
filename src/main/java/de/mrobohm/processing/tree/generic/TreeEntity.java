package de.mrobohm.processing.tree.generic;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public sealed interface TreeEntity<TContent> extends Comparable<TreeEntity<TContent>> permits TreeLeaf, TreeNode {
    TContent content();

    default TreeNode<TContent> withChildren(List<TreeEntity<TContent>> newChildList) {
        return new TreeNode<>(content(), newChildList);
    }

    @Override
    default int compareTo(@NotNull TreeEntity te) {
        return this.toString().compareTo(te.toString());
    }
}
