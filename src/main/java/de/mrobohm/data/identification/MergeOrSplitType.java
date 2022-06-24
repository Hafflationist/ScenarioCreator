package de.mrobohm.data.identification;

public enum MergeOrSplitType {
    And,    // a content record of the predecessor must have been in both of the merged columns (e.g. lastname, firstname)
    Xor,    // a content record of the predecessor must have been in exactly one of the two merged columns (e.g. inheritance)
    Other   // merge/split mode cannot be determined
}
