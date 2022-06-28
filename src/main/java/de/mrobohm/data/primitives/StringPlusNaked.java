package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;


public record StringPlusNaked(String rawString, Language language) implements StringPlus {
    public StringPlusNaked withRawString(String newRawString) {
        return new StringPlusNaked(newRawString, language);
    }
}