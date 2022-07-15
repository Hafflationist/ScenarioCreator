package de.mrobohm.processing.tree;

import de.mrobohm.data.Schema;

import java.util.Set;

public record TreeNode(Schema schema, Set<TreeEntity> childSet) implements TreeEntity {
}
