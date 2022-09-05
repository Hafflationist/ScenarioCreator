package de.mrobohm.data.identification;

public record IdPart(Id predecessorId, int extensionNumber, MergeOrSplitType splitType) implements Id {
    @Override
    public String toString() {
        final var preStr = predecessorId().toString();
        final var type = switch (splitType()) {
            case Xor -> "+";
            case And -> "*";
            default -> ".";
        };
        return "(" + preStr + ")" + type + extensionNumber();
    }
}
