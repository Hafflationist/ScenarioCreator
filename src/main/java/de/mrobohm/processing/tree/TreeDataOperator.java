package de.mrobohm.processing.tree;

import de.mrobohm.utils.SSet;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class TreeDataOperator {
    private TreeDataOperator() {
    }

    static <TContent> TreeEntity<TContent> replaceTreeEntity(
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
                final var childSet = tn.childSet();
                final var newChildSet = childSet.stream()
                        .map(child -> replaceTreeEntity(child, oldEntity, newEntity))
                        .collect(Collectors.toCollection(TreeSet::new));

                if (newChildSet.equals(childSet)) {
                    yield root;
                }
                yield root.withChildren(newChildSet);
            }
        };
    }


    static <TContent> SortedSet<TreeEntity<TContent>> getAllTreeEntitySet(TreeEntity<TContent> te) {
        return switch (te) {
            case TreeLeaf<TContent> tl -> SSet.of((TreeEntity<TContent>) tl);
            case TreeNode<TContent> tn -> {
                final var children = tn.childSet().parallelStream()
                        .flatMap(tnc -> getAllTreeEntitySet(tnc).stream());
                yield Stream.concat(Stream.of(tn), children).collect(Collectors.toCollection(TreeSet::new));
            }
        };
    }
}
