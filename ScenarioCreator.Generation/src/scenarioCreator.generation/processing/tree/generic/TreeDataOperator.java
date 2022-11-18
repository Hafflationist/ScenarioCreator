package scenarioCreator.generation.processing.tree.generic;

import java.util.List;
import java.util.stream.Stream;

public final class TreeDataOperator {
    private TreeDataOperator() {
    }

    public static <TContent> TreeEntity<TContent> replaceTreeEntity(
            TreeEntity<TContent> root,
            TreeEntity<TContent> oldEntity,
            TreeEntity<TContent> newEntity
    ) {
        if (root.equals(oldEntity)) {
            return newEntity;
        }
        return switch (root) {
            case TreeLeaf ignore -> root;
            case TreeNode<TContent> tn -> {
                final var childList = tn.childList();
                final var newChildList = childList.parallelStream()
                        .map(child -> replaceTreeEntity(child, oldEntity, newEntity))
                        .toList();
                if (newChildList.equals(childList)) {
                    yield root;
                }
                yield root.withChildren(newChildList);
            }
        };
    }


    public static <TContent> List<TreeEntity<TContent>> getAllTreeEntityList(TreeEntity<TContent> te) {
        return switch (te) {
            case TreeLeaf<TContent> tl -> List.of((TreeEntity<TContent>) tl);
            case TreeNode<TContent> tn -> {
                final var children = tn.childList().parallelStream()
                        .flatMap(tnc -> getAllTreeEntityList(tnc).stream());
                yield Stream.concat(Stream.of(tn), children).toList();
            }
        };
    }
}