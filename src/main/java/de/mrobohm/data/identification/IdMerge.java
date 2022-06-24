package de.mrobohm.data.identification;

public record IdMerge(Id predecessorId1, Id predecessorId2, MergeOrSplitType mergeType) implements Id {
}
