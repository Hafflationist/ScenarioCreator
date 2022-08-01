package de.mrobohm.data.identification;

public record IdPart(Id predecessorId, int extensionNumber, MergeOrSplitType splitType) implements Id {
    @Override
    public String toString() {
        var preStr = predecessorId().toString();
        var type = switch (splitType()) {
            case Xor -> "+";
            case And -> "*";
            default -> ".";
        };
        return "(" + preStr + ")" + type + extensionNumber();
    }
}
