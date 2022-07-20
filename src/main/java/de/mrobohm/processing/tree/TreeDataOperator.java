package de.mrobohm.processing.tree;

import java.util.stream.Collectors;

final class TreeDataOperator {
    private TreeDataOperator() {
    }

    static <TContent> TreeEntity<TContent> replaceTreeEntity(
            TreeEntity<TContent> root,
            TreeEntity<TContent> oldEntity,
            TreeEntity<TContent> newEntity
    ) {
        // TODO: TEST ME!!!
        if (root.equals(oldEntity)) {
            return newEntity;
        }
        return switch (root) {
            case TreeLeaf ignore -> root;
            case TreeNode<TContent> tn -> {
                var childSet = tn.childSet();
                var newChildSet = childSet.stream()
                        .map(child -> replaceTreeEntity(child, oldEntity, newEntity))
                        .collect(Collectors.toSet());

                if (newChildSet.equals(childSet)) {
                    yield root;
                }
                yield root.withChildren(newChildSet);
            }
        };
    }
}
