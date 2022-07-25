package de.mrobohm.processing.tree;

import java.util.SortedSet;

public record TreeNode<TContent>(TContent content, SortedSet<TreeEntity<TContent>> childSet) implements TreeEntity<TContent> {
}
