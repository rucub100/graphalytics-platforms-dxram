/*
 * Copyright 2015 Delft University of Technology
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
package science.atlarge.graphalytics.dxram.algorithms.bfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.TaskListener;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxram.ms.TaskScriptState;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import science.atlarge.graphalytics.domain.algorithms.AlgorithmParameters;
import science.atlarge.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.dxram.DxramConfiguration;
import science.atlarge.graphalytics.dxram.ProcTimeLog;
import science.atlarge.graphalytics.dxram.graph.data.GraphRootList;
import science.atlarge.graphalytics.dxram.graph.data.Vertex;
import science.atlarge.graphalytics.dxram.graph.load.GraphLoadBFSRootListTask;
import science.atlarge.graphalytics.dxram.graph.load.GraphLoadOrderedEdgeListTask;
import science.atlarge.graphalytics.dxram.graph.load.GraphLoadPartitionIndexTask;
import science.atlarge.graphalytics.dxram.graph.load.oel.GraphalyticsOrderedEdgeList;
import science.atlarge.graphalytics.dxram.job.DxramJob;

/**
 * DXRAM implementation of the Breadth-first Search algorithm.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 */
public final class BreadthFirstSearchJob extends DxramJob {

	private static final Logger LOG = LogManager.getLogger();

	private final long sourceVertex;
	private transient GraphAlgorithmBFSTask bfsTask;
	private transient GraphLoadPartitionIndexTask gpiTask;
	private transient GraphLoadOrderedEdgeListTask oelTask;

	/**
	 * Creates a new BreadthFirstSearchJob object with all mandatory parameters specified.
	 *
	 * @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public BreadthFirstSearchJob(RunSpecification runSpecification, DxramConfiguration platformConfig) {
		super(runSpecification, platformConfig);

		AlgorithmParameters parameters = runSpecification.getBenchmarkRun().getAlgorithmParameters();
		this.sourceVertex = ((BreadthFirstSearchParameters)parameters).getSourceVertex();
	}

	private void submitBFSTask() {
		final AtomicBoolean finished = new AtomicBoolean(false); 
		final TaskListener taskListener = new TaskListener() {
			@Override
			public void taskCompleted(TaskScriptState p_taskScriptState) {
				finished.set(true);
			}

			@Override
			public void taskBeforeExecution(TaskScriptState p_taskScriptState) {}
		};

		MasterSlaveComputeService ms = getService(MasterSlaveComputeService.class);
		bfsTask = new GraphAlgorithmBFSTask();
		TaskScript taskScript = new TaskScript(bfsTask);
		ms.submitTaskScript(taskScript, (short)0, taskListener);

		// wait till finished
		while(!finished.get()) {
			try { 
				Thread.sleep(100);
			} catch (Exception ignore) {}
		}
	}

	private void submitGPITask() {
		final AtomicBoolean finished = new AtomicBoolean(false); 
		final TaskListener taskListener = new TaskListener() {
			@Override
			public void taskCompleted(TaskScriptState p_taskScriptState) {
				finished.set(true);
			}

			@Override
			public void taskBeforeExecution(TaskScriptState p_taskScriptState) {}
		};

		MasterSlaveComputeService ms = getService(MasterSlaveComputeService.class);
		gpiTask = new GraphLoadPartitionIndexTask();
		gpiTask.setPartitionIndexFilePath(vertexPath + ".gpi");
		TaskScript taskScript = new TaskScript(gpiTask);
		ms.submitTaskScript(taskScript, (short)0, taskListener);

		// wait till finished
		while(!finished.get()) {
			try { 
				Thread.sleep(100);
			} catch (Exception ignore) {}
		}
	}

	private void loadRootList() {
		NameserviceService nameserviceService = getService(NameserviceService.class);
		ChunkService chunkService = getService(ChunkService.class);
		ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);

		GraphRootList rootList = new GraphRootList(ChunkID.INVALID_ID,
				new long[] { GraphalyticsOrderedEdgeList.VERTEX_ID_TO_CID.get(sourceVertex) }); // index starts with 0; offset per slave?

        // store the root list for our current compute group
        if (chunkLocalService.createLocal().create(rootList) != 1) {
            LOG.error("Creating chunk for root list failed");
        }

        if (!chunkService.put().put(rootList)) {
            LOG.error("Putting root list failed");
        }

        // register chunk at nameservice that other slaves can find it
        nameserviceService.register(rootList, GraphLoadBFSRootListTask.MS_BFS_ROOTS + "0");
        LOG.info(
        		"Successfully loaded and stored root list, nameservice entry name %s:\n%s",
        		GraphLoadBFSRootListTask.MS_BFS_ROOTS + "0",
                rootList);
	}

	private void loadOELTask() {
		final AtomicBoolean finished = new AtomicBoolean(false); 
		final TaskListener taskListener = new TaskListener() {
			@Override
			public void taskCompleted(TaskScriptState p_taskScriptState) {
				finished.set(true);
			}

			@Override
			public void taskBeforeExecution(TaskScriptState p_taskScriptState) {}
		};

		MasterSlaveComputeService ms = getService(MasterSlaveComputeService.class);
		oelTask = new GraphLoadOrderedEdgeListTask();
		oelTask.setLoadVertexPath(vertexPath);
		oelTask.setLoadEdgePath(edgePath);
		TaskScript taskScript = new TaskScript(oelTask);
		ms.submitTaskScript(taskScript, (short)0, taskListener);

		// wait till finished
		while(!finished.get()) {
			try { 
				Thread.sleep(100);
			} catch (Exception ignore) {}
		}
	}

	@Override
	protected void run() {
		LOG.error("RUN DXGraph-BFS algorithm...");
//		if (vertexPath.contains("example-directed")) {
//			LOG.error("SKIP DIRECTED GRAPH");
//			return;
//		}
		// reserve local ids (CIDs)
		ChunkLocalService cls = getService(ChunkLocalService.class);
		cls.reserveLocal().reserve(600000000);
		// run task to load the graph partition index
		submitGPITask();
		// load graph into runner DXRAM instance
		loadOELTask();
		// load root vertex into the temporary storage
		loadRootList();
		// run the BFSTask
		// define runner as slave to run the task locally (debugging)
		ProcTimeLog.start();
		submitBFSTask();
		ProcTimeLog.end();
		// gather the results and create "expected" output
		final StringBuilder output = new StringBuilder();

		for (long vid : GraphalyticsOrderedEdgeList.VERTEX_ID_TO_CID.keySet()) {
		    final long cid = GraphalyticsOrderedEdgeList.VERTEX_ID_TO_CID.get(vid);
			Vertex v = new Vertex();
			v.setID(cid);
			cls.getLocal().get(v);
			long depth = v.getDepth();
			if (depth == -1) depth = Long.MAX_VALUE;
			output.append(String.format("%d %d", vid, depth));
			output.append('\n');
			//LOG.info(String.format("Vertex %d has depth %d", vid, v.getUserData()));
		}

		try {
            Files.write(
                    Paths.get(outputPath),
                    output.toString().getBytes(),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
	}
}
