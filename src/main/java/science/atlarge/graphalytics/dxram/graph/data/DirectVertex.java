/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package science.atlarge.graphalytics.dxram.graph.data;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 05.03.2019
 *
 */
public final class DirectVertex implements AutoCloseable {

    private static final int OFFSET_DEPTH = 0;
    private static final int OFFSET_NEIGHBORS_LENGTH = 4;
    private static final int OFFSET_NEIGHBORS_CID = 8;
    private static final int OFFSET_NEIGHBORS_ADDR = 16;

    private static boolean INITIALIZED = false;
    private static ChunkLocalService CHUNK_LOCAL_SERVICE = null;
    private static ChunkService CHUNK_SERVICE = null;

    public static void init(DXRAMServiceAccessor p_accessor) {
        if (!INITIALIZED) {
            CHUNK_LOCAL_SERVICE = p_accessor.getService(ChunkLocalService.class);
            CHUNK_SERVICE = p_accessor.getService(ChunkService.class);
            INITIALIZED = true;
        }
    }

    public static int size() {
        return Integer.BYTES + // depth
                Integer.BYTES + Long.BYTES; // neighbors (length + first node)
    }

    public static DirectVertex use(long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(p_cid).getAddress();
        return new DirectVertex(p_cid, address);
    }

    private final long m_cid;
    private final long m_address;

    private DirectVertex(final long p_cid, final long p_address) {
        m_cid = p_cid;
        m_address = p_address;
    }

    public int getDepth() {
        return CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(m_address, OFFSET_DEPTH);
    }

    public void setDepth(int p_depth) {
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(m_address, OFFSET_DEPTH, p_depth);
    }

    public long[] getNeighbors() {
        int count = CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(m_address, OFFSET_NEIGHBORS_LENGTH);

        if (count == 0) {
            return new long[0];
        }

        long address = CHUNK_LOCAL_SERVICE.rawReadLocal().readLong(m_address, OFFSET_NEIGHBORS_ADDR);
        return CHUNK_LOCAL_SERVICE.rawReadLocal().readLongArray(address, 0, count);
    }

    public void setNeighbors(long[] p_neighbors) {
        // remove old array
        long cid = CHUNK_LOCAL_SERVICE.rawReadLocal().readLong(m_address, OFFSET_NEIGHBORS_CID);
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(cid);
        CHUNK_SERVICE.remove().remove(cid);
        // set new array
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(m_address, OFFSET_NEIGHBORS_LENGTH, p_neighbors.length);
        // TODO: fix dxram API, for now this workaround
        long [] newCID = new long[1];
        int created = CHUNK_LOCAL_SERVICE.createLocal().create(newCID, 1, p_neighbors.length);
        if (created != 1) {
            throw new RuntimeException("Failed to create a new chunk!");
        }
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(m_address, OFFSET_NEIGHBORS_CID, newCID[0]);
        long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(newCID[0]).getAddress();
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(m_address, OFFSET_NEIGHBORS_ADDR, address);
    }

    @Override
    public void close() throws Exception {
        // unpin via CID instead of address is faster
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(m_cid);
    }
}
