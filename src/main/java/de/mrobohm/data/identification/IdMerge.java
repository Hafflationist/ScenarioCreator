package de.mrobohm.data.identification;

public record IdMerge(Id predecessorId1, Id predecessorId2, MergeOrSplitType mergeType) implements Id {
    @Override
    public String toString() {
        var preStr1 = predecessorId1().toString();
        var preStr2 = predecessorId2().toString();
        var type = switch (mergeType()) {
            case Xor -> "+";
            case And -> "*";
            default -> ".";
        };
        return "[" + preStr1 + "|" + type + "|" + preStr2 + "]";
    }
}