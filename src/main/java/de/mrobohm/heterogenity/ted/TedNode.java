package de.mrobohm.heterogenity.ted;

import java.util.List;

class TedNode {

    final String label; // node label
    int index; // preorder index
    // note: trees need not be binary
    List<TedNode> children = List.of();
    TedNode leftmost; // used by the recursive O(n) leftmost() function

    TedNode(String label) {
        this.label = label;
    }
}
