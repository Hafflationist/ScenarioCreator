package de.mrobohm.processing.tree;

import java.util.Set;

public record TreeNode<TContent>(TContent content, Set<TreeEntity<TContent>> childSet) implements TreeEntity<TContent> {
}
