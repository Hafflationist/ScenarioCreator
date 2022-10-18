package de.mrobohm.processing.tree.generic;

public record TreeLeaf<TContent>(TContent content) implements TreeEntity<TContent> {
}
