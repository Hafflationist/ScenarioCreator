package de.mrobohm.heterogeneity.constraintBased.regexy.minimizer;

import de.mrobohm.heterogeneity.constraintBased.regexy.StateDet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class Group extends ArrayList<StateDet> {

    private int id;

    Group(int i) {
        id = i;
    }

    Group(Collection<StateDet> g, int i) {
        id=i;
        this.addAll(g);
    }

    @Override
    public boolean add(StateDet state) {
        if(this.contains(state))
            return false;
        return super.add(state);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Group without(StateDet state) {
        if(!this.contains(state)) return this;
        return new Group(this.stream().filter(s -> !s.equals(state)).toList(), this.id);
    }

    public Group intersection(Set<StateDet> stateDets) {
        if(this.stream().allMatch(stateDets::contains)) return this;
        return new Group(this.stream().filter(stateDets::contains).toList(), this.id);
    }

    @Override
    public String toString() {
        return "Group " + id + "\t" + super.toString();
    }
}