package scenarioCreator.generation.processing.tree.generic;

public record TreeLeaf<TContent>(TContent content) implements TreeEntity<TContent> {
}
