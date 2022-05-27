package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;


public record StringPlusNaked(String rawString, Language language) implements StringPlus {
}