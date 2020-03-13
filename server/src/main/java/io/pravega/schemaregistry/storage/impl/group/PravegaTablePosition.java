package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.controller.store.stream.Version;
import io.pravega.schemaregistry.storage.Position;

public class PravegaTablePosition implements Position<Version> {
    private final Version pos;

    public PravegaTablePosition(Version pos) {
        this.pos = pos;
    }

    @Override
    public Version getPosition() {
        return pos;
    }
}
