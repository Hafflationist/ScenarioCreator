package de.mrobohm.processing.tree;

public record TreeLeaf<TContent>(TContent content) implements TreeEntity<TContent> {
}
