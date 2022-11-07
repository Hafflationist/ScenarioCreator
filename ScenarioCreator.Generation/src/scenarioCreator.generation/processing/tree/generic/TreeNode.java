package scenarioCreator.generation.processing.tree.generic;

import java.util.List;

// I've chosen LIST instead of SET because it prevents recursive hashcode/compare calculation
public record TreeNode<TContent>(TContent content, List<TreeEntity<TContent>> childList) implements TreeEntity<TContent> {
}
