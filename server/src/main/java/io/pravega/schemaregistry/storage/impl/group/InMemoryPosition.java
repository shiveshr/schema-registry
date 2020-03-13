package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.schemaregistry.storage.Position;

public class InMemoryPosition implements Position<Integer> {
    private final int pos;

    public InMemoryPosition(int pos) {
        this.pos = pos;
    }

    @Override
    public Integer getPosition() {
        return pos;
    }
}
