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

import de.hhu.bsinfo.dxmem.core.Address;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.ServiceProvider;

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

    public static void init(ServiceProvider p_accessor) {
        if (!INITIALIZED) {
            CHUNK_LOCAL_SERVICE = p_accessor.getService(ChunkLocalService.class);
            CHUNK_SERVICE = p_accessor.getService(ChunkService.class);
            INITIALIZED = true;
        }
    }

    public static int size() {
        return Integer.BYTES + // depth
                Integer.BYTES + Long.BYTES + Long.BYTES; // neighbors (length + cid + addr)
    }

    public static long create() {
        long[] cid = new long[1];
        int created = CHUNK_LOCAL_SERVICE.createLocal().create(cid, 1, size());
        if (created != 1) {
            throw new RuntimeException("Failed to create a new chunk!");
        }
        // initialize with default values
        long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(cid[0]).getAddress();
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_DEPTH, -1);
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_NEIGHBORS_LENGTH, 0);
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, ChunkID.INVALID_ID);
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, Address.INVALID);
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(cid[0]);
        return cid[0];
    }

    public static long[] create(final int p_count) {
        long[] cids = new long[p_count];
        int created = CHUNK_LOCAL_SERVICE.createLocal().create(cids, p_count, size());
        if (created != p_count) {
            throw new RuntimeException(String.format("Failed to create %d chunks!", p_count));
        }
        // initialize with default values
        for (int i = 0; i < p_count; i++) {
            long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(cids[i]).getAddress();
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_DEPTH, -1);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_NEIGHBORS_LENGTH, 0);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, ChunkID.INVALID_ID);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, Address.INVALID);
            CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(cids[i]);            
        }
        return cids;
    }

    public static long create(final Vertex p_vertex) {
        long[] cid = new long[1];
        int created = CHUNK_LOCAL_SERVICE.createLocal().create(cid, 1, size());
        if (created != 1) {
            throw new RuntimeException("Failed to create a new chunk!");
        }
        // initialize with default values
        long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(cid[0]).getAddress();
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_DEPTH, p_vertex.getDepth());
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_NEIGHBORS_LENGTH, p_vertex.getNeighbors().length);
        if (p_vertex.getNeighbors().length == 0) {
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, ChunkID.INVALID_ID);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, Address.INVALID);
        } else {
            long [] newCID = new long[1];
            created = CHUNK_LOCAL_SERVICE.createLocal().create(newCID, 1, Long.BYTES * p_vertex.getNeighbors().length);
            if (created != 1) {
                throw new RuntimeException("Failed to create a new chunk!");
            }
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, newCID[0]);
            long address2 = CHUNK_LOCAL_SERVICE.pinningLocal().pin(newCID[0]).getAddress();
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, address2);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLongArray(address2, 0, p_vertex.getNeighbors());
        }
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(cid[0]);
        return cid[0];
    }

    public static long[] create(final Vertex[] p_vertices) {
        long[] cids = new long[p_vertices.length];
        int created = CHUNK_LOCAL_SERVICE.createLocal().create(cids, p_vertices.length, size());
        if (created != p_vertices.length) {
            throw new RuntimeException(String.format("Failed to create %d chunks!", p_vertices.length));
        }
        // initialize with default values
        for (int i = 0; i < p_vertices.length; i++) {
            long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(cids[i]).getAddress();
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_DEPTH, p_vertices[i].getDepth());
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_NEIGHBORS_LENGTH, p_vertices[i].getNeighbors().length);
            if (p_vertices[i].getNeighbors().length == 0) {
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, ChunkID.INVALID_ID);
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, 0);
            } else {
                long [] newCID = new long[1];
                created = CHUNK_LOCAL_SERVICE.createLocal().create(newCID, 1, Long.BYTES * p_vertices[i].getNeighbors().length);
                if (created != 1) {
                    throw new RuntimeException("Failed to create a new chunk!");
                }
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, newCID[0]);
                long address2 = CHUNK_LOCAL_SERVICE.pinningLocal().pin(newCID[0]).getAddress();
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, address2);
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLongArray(address2, 0, p_vertices[i].getNeighbors());
            }
            CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(cids[i]);            
        }
        return cids;
    }

    public static long[] createReserved(final Vertex[] p_vertices) {
        long[] cids = new long[p_vertices.length];
        int[] sizes = new int[p_vertices.length];
        for (int i = 0; i < p_vertices.length; i++) {
            cids[i] = p_vertices[i].getID();
            sizes[i] = size();
        }
        int created = CHUNK_LOCAL_SERVICE.createReservedLocal().create(cids, cids.length, sizes);
        if (created != p_vertices.length) {
            throw new RuntimeException(String.format("Failed to create %d chunks!", p_vertices.length));
        }
        // initialize with default values
        for (int i = 0; i < p_vertices.length; i++) {
            long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(cids[i]).getAddress();
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_DEPTH, p_vertices[i].getDepth());
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_NEIGHBORS_LENGTH, p_vertices[i].getNeighbors().length);
            if (p_vertices[i].getNeighbors().length == 0) {
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, ChunkID.INVALID_ID);
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, 0);
            } else {
                long [] newCID = new long[1];
                created = CHUNK_LOCAL_SERVICE.createLocal().create(newCID, 1, Long.BYTES * p_vertices[i].getNeighbors().length);
                if (created != 1) {
                    throw new RuntimeException("Failed to create a new chunk!");
                }
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, newCID[0]);
                long address2 = CHUNK_LOCAL_SERVICE.pinningLocal().pin(newCID[0]).getAddress();
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, address2);
                CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLongArray(address2, 0, p_vertices[i].getNeighbors());
            }          
        }
        return cids;
    }

    public static DirectVertex use(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(p_cid).getAddress();
        return new DirectVertex(p_cid, address);
    }

    public static void remove(final long p_cid) {
        long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(p_cid).getAddress();
        int neighborsLength = CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(address, OFFSET_NEIGHBORS_LENGTH);
        if (neighborsLength > 0) {
            long neighborsCID = CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(address, OFFSET_NEIGHBORS_CID);
            CHUNK_SERVICE.remove().remove(neighborsCID);
        }
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(p_cid);

        int removed = CHUNK_SERVICE.remove().remove(p_cid);
        if (removed != 1) {
            throw new RuntimeException(String.format("Failed to remove the chunk 0x%08X", p_cid));
        }
    }

    public static void remove(final long[] p_cids) {
        for (int i = 0; i < p_cids.length; i++) {
            long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(p_cids[i]).getAddress();
            int neighborsLength = CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(address, OFFSET_NEIGHBORS_LENGTH);
            if (neighborsLength > 0) {
                long neighborsCID = CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(address, OFFSET_NEIGHBORS_CID);
                CHUNK_SERVICE.remove().remove(neighborsCID);
            }
            CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(p_cids[i]);
        }
        int removed = CHUNK_SERVICE.remove().remove(p_cids);
        if (removed != p_cids.length) {
            throw new RuntimeException(String.format("Could not remove all chunks: %d of %d removed", removed, p_cids.length));
        }
    }

    public static int getDepth(final long p_cid) {
        return CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(
                CHUNK_LOCAL_SERVICE.pinningLocal().translate(p_cid),
                OFFSET_DEPTH);
    }

    public static void setDepth(final long p_cid, final int p_depth) {
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(
                CHUNK_LOCAL_SERVICE.pinningLocal().translate(p_cid),
                OFFSET_DEPTH, p_depth);
    }

    public static int getNeighborsLength(final long p_cid) {
        return CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(
                CHUNK_LOCAL_SERVICE.pinningLocal().translate(p_cid),
                OFFSET_NEIGHBORS_LENGTH);
    }

    // TODO getNeighbor(index)
    public static long[] getNeighbors(final long p_cid) {
        final long address = CHUNK_LOCAL_SERVICE.pinningLocal().translate(p_cid);
        int count = CHUNK_LOCAL_SERVICE.rawReadLocal().readInt(
                address,
                OFFSET_NEIGHBORS_LENGTH);

        if (count == 0) {
            return new long[0];
        }

        return CHUNK_LOCAL_SERVICE.rawReadLocal().readLongArray(
                CHUNK_LOCAL_SERVICE.rawReadLocal().readLong(address, OFFSET_NEIGHBORS_ADDR),
                0,
                count);
    }

    public static void setNeighbors(final long p_cid, final long[] p_neighbors) {
        long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(p_cid).getAddress();
        // remove old array
        long cid = CHUNK_LOCAL_SERVICE.rawReadLocal().readLong(address, OFFSET_NEIGHBORS_CID);
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(cid);
        CHUNK_SERVICE.remove().remove(cid);
        // set new array
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(address, OFFSET_NEIGHBORS_LENGTH, p_neighbors.length);
        if (p_neighbors.length == 0) {
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, ChunkID.INVALID_ID);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, Address.INVALID);
        } else {
            // TODO: fix dxram API, for now this workaround
            long [] newCID = new long[1];
            int created = CHUNK_LOCAL_SERVICE.createLocal().create(newCID, 1, Long.BYTES * p_neighbors.length);
            if (created != 1) {
                throw new RuntimeException("Failed to create a new chunk!");
            }
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_CID, newCID[0]);
            long address2 = CHUNK_LOCAL_SERVICE.pinningLocal().pin(newCID[0]).getAddress();
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(address, OFFSET_NEIGHBORS_ADDR, address2);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLongArray(address2, 0, p_neighbors);
        }
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(p_cid);
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

    public void setDepth(final int p_depth) {
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

    public void setNeighbors(final long[] p_neighbors) {
        // remove old array
        long cid = CHUNK_LOCAL_SERVICE.rawReadLocal().readLong(m_address, OFFSET_NEIGHBORS_CID);
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(cid);
        CHUNK_SERVICE.remove().remove(cid);
        // set new array
        CHUNK_LOCAL_SERVICE.rawWriteLocal().writeInt(m_address, OFFSET_NEIGHBORS_LENGTH, p_neighbors.length);
        if (p_neighbors.length == 0) {
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(m_address, OFFSET_NEIGHBORS_CID, ChunkID.INVALID_ID);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(m_address, OFFSET_NEIGHBORS_ADDR, Address.INVALID);
        } else {
            // TODO: fix dxram API, for now this workaround
            long [] newCID = new long[1];
            int created = CHUNK_LOCAL_SERVICE.createLocal().create(newCID, 1, Long.BYTES * p_neighbors.length);
            if (created != 1) {
                throw new RuntimeException("Failed to create a new chunk!");
            }
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(m_address, OFFSET_NEIGHBORS_CID, newCID[0]);
            long address = CHUNK_LOCAL_SERVICE.pinningLocal().pin(newCID[0]).getAddress();
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLong(m_address, OFFSET_NEIGHBORS_ADDR, address);
            CHUNK_LOCAL_SERVICE.rawWriteLocal().writeLongArray(address, 0, p_neighbors);
        }
    }

    @Override
    public void close() throws Exception {
        // unpin via CID instead of address is faster
        CHUNK_LOCAL_SERVICE.pinningLocal().unpinCID(m_cid);
    }
}
