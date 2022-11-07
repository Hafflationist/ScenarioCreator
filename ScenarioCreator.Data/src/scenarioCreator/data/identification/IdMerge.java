package scenarioCreator.data.identification;

public record IdMerge(Id predecessorId1, Id predecessorId2, MergeOrSplitType mergeType) implements Id {
    @Override
    public String toString() {
        final var preStr1 = predecessorId1().toString();
        final var preStr2 = predecessorId2().toString();
        final var type = switch (mergeType()) {
            case Xor -> "+";
            case And -> "*";
            default -> ".";
        };
        return "[" + preStr1 + "|" + type + "|" + preStr2 + "]";
    }
}