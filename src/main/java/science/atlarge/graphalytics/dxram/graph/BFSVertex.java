/* 
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, 
 * Institute of Computer Science, Department Operating Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package science.atlarge.graphalytics.dxram.graph;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Vertex chunk, optimized for BFS.
 * 
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, Dec 30, 2018
 *
 */
public final class BFSVertex extends AbstractChunk {

	private long id = -1;
	private long depth = -1;

	// outgoing neighbors in case of directed graphs; all neighbors otherwise
	private long[] neighbors = null;

	@Override
	public int sizeofObject() {
		return Long.BYTES * (2 + neighbors.length);
	}

	@Override
	public void importObject(Importer p_importer) {
		this.id = p_importer.readLong(this.id);
		this.depth = p_importer.readLong(this.depth);
		this.neighbors = p_importer.readLongArray(this.neighbors);
	}

	@Override
	public void exportObject(Exporter p_exporter) {
		p_exporter.writeLong(this.id);
		p_exporter.writeLong(this.depth);
		p_exporter.writeLongArray(this.neighbors);
	}
}
