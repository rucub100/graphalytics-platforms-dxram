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
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, Jan 1, 2019
 *
 */
public class Graph extends AbstractChunk{

	/**
	 * Graph page table with 65536 entries.
	 * Map vertex id to CID.
	 * 
	 * 4 levels
	 * |--------|--------|--------|--------|
	 * |   16   |   16   |   16   |   16   |
	 * |--------|--------|--------|--------|
	 * |  head  |2nd lvl.|3rd lvl.|  CIDs  |
	 * 
	 */
	long headId = ChunkID.INVALID_ID;
	long[] vertices = null;
	boolean constructed = false;

	@Override
	public void importObject(Importer p_importer) {
		// TODO Auto-generated method stub
	}

	@Override
	public void exportObject(Exporter p_exporter) {
		// TODO Auto-generated method stub
	}

	@Override
	public int sizeofObject() {
		// TODO Auto-generated method stub
		return 0;
	}
}
