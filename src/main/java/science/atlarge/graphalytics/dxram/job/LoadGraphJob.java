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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 *
 */
public class LoadGraphJob extends GraphalyticsAbstractJob {

	public static final short TYPE_ID = 1;

	@Override
	public short getTypeID() {
		return TYPE_ID;
	}

	@Override
	protected void execute(short p_nodeID, long[] p_chunkIDs) {
		ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);

		// TODO create a graph chunk (set/list of vertices + set/list of edges)

		try (Stream<String> stream = Files.lines(Paths.get(vertexPath), StandardCharsets.US_ASCII)) {
			stream.forEach(new Consumer<String>() {
				@Override
				public void accept(String t) {
					// TODO create chunk and add in graph
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
