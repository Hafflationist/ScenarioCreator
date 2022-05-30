package de.mrobohm.operations.linguistic.helpers.biglingo;

public record InterLingoRecord(int num, PartOfSpeech partOfSpeech) {
    public enum PartOfSpeech {
        NOUN,
        VERB,
        ADJECTIVE
    }
}