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

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * 
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 05.03.2019
 *
 */
public final class Vertex extends AbstractChunk {

    private int m_depth;
    private long[] m_neighbors;

    public Vertex() {
        m_depth = -1; // provide default value
        m_neighbors = new long[0];
    }

    public int getDepth() {
        return m_depth;
    }

    public void setDepth(int depth) {
        m_depth = depth;
    }

    public long[] getNeighbors() {
        return m_neighbors;
    }

    public void setNeighbors(long[] p_neighbors) {
        m_neighbors = p_neighbors;
    }

    @Override
    public void importObject(Importer p_importer) {
        m_depth = p_importer.readInt(m_depth);
        m_neighbors = p_importer.readLongArray(m_neighbors);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + ObjectSizeUtil.sizeofLongArray(m_neighbors);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_depth);
        p_exporter.writeLongArray(m_neighbors);
    }
    
}
