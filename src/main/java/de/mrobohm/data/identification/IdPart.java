package de.mrobohm.data.identification;

public record IdPart(Id predecessorId, int extensionNumber) implements Id {
}
