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
package science.atlarge.graphalytics.dxram.job;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 *
 */
public class DropAllChunksJob extends AbstractJob {

	public static final short TYPE_ID = 2;
	
	@Override
	public short getTypeID() {
		return TYPE_ID;
	}
	
	@Override
	protected void execute(short p_nodeID, long[] p_chunkIDs) {
		// TODO Auto-generated method stub
		ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
		ChunkService chunkService = getService(ChunkService.class);
		
		// TODO remove all chunks
	}

}