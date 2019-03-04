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

import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.dxram.DxramConfiguration;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService.StatusMaster;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;

import java.util.List;

/**
 * Base class for all jobs in the platform driver. Configures and executes a platform job using the parameters
 * and executable specified by the subclass for a specific algorithm.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 */
public abstract class DxramJob extends GraphalyticsAbstractJob {

	private static final Logger LOG = LogManager.getLogger();

	/**
     * Initializes the platform job with its parameters.
	 * @param runSpecification the benchmark run specification.
	 * @param platformConfig the platform configuration.
	 * @param inputPath the file path of the input graph dataset.
	 * @param outputPath the file path of the output graph dataset.
	 */
	public DxramJob(RunSpecification runSpecification, DxramConfiguration platformConfig) {

		BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();

		this.jobId = benchmarkRun.getId();
		this.logPath = benchmarkRunSetup.getLogDir().resolve("platform").toString();
		this.vertexPath = runSpecification
			.getRuntimeSetup()
			.getLoadedGraph()
			.getVertexPath();
		this.edgePath = runSpecification
				.getRuntimeSetup()
				.getLoadedGraph()
				.getEdgePath();
		this.outputPath = benchmarkRunSetup
			.getOutputDir()
			.resolve(benchmarkRun.getName())
			.toAbsolutePath()
			.toString();

		this.platformConfig = platformConfig;
	}

	protected abstract void run();

	protected void load(JobService jobService, List<Short> storageNodes) {
		LOG.info(String.format(
				"Create load graph job: id=%s, log-path=%s, vertex-path=%s, edge-path=%s, output-path=%s",
				this.jobId,
				this.logPath,
				this.vertexPath,
				this.edgePath,
				this.outputPath));
		LoadGraphJob loadGraphJob = new LoadGraphJob(
				this.jobId,
				this.logPath,
				this.vertexPath,
				this.edgePath,
				this.outputPath,
				this.platformConfig);

		for (Short nodeId : storageNodes) {
			LOG.info(String.format("Push load graph job to remote node %d...", nodeId));
			jobService.pushJobRemote(loadGraphJob, nodeId);
		}

		LOG.info("Wait for all remote graph loading jobs to finish...");
		jobService.waitForRemoteJobsToFinish();
		LOG.info("All remote graph loading jobs finished");
	}

	protected void unload(JobService jobService, List<Short> storageNodes) {
		LOG.info(String.format(
				"Create unload graph job: id=%s, log-path=%s, vertex-path=%s, edge-path=%s, output-path=%s",
				this.jobId,
				this.logPath,
				this.vertexPath,
				this.edgePath,
				this.outputPath));
		DropAllChunksJob dropAllChunksJob = new DropAllChunksJob(
				this.jobId,
				this.logPath,
				this.vertexPath,
				this.edgePath,
				this.outputPath,
				this.platformConfig);

		for (Short nodeId : storageNodes) {
			LOG.info(String.format("Push unload graph job to remote node %d...", nodeId));
			jobService.pushJobRemote(dropAllChunksJob, nodeId);
		}

		LOG.info("Wait for all remote graph unloading jobs to finish...");
		jobService.waitForRemoteJobsToFinish();
		LOG.info("All remote graph unloading jobs finished");
	}

	/**
	 * Executes the platform job with the pre-defined parameters.
	 *
	 */
	@Override
	public void execute() {
		LOG.info(String.format("Execute benchmark job %s", this.jobId));

		BootService bootService = getService(BootService.class);
		//JobService jobService = getService(JobService.class);
		MasterSlaveComputeService ms = getService(MasterSlaveComputeService.class);

		// get a list of all nodes for storage and computations
		List<Short> storageNodes = bootService
				.getSupportingNodes(NodeCapabilities.STORAGE);
		LOG.info(String.format("Found %d nodes with STORAGE capability support:", storageNodes.size()));
		
		StatusMaster stat = ms.getStatusMaster((short)0);
		for (short node : storageNodes) {
			String ms_role = "none";
			if (stat.getMasterNodeId() == node) {
				ms_role = "master";
			} else if (stat.getConnectedSlaves().contains(node)) {
				ms_role = "slave";
			}
			LOG.info(String.format(
					"Node %d, %s, addr=%s, [MS-role=%s]",
					node,
					bootService.getNodeRole(node).toString(),
					bootService.getNodeAddress(node).toString(),
					ms_role));
		}

		if (storageNodes.isEmpty()) {
			LOG.error("No supporting (STORAGE) nodes found");
		}

		// exclude this node which controls the benchmark and coordinates the runs
		LOG.info(String.format("Exclude node [%d] which controls the benchmark", bootService.getNodeID()));
		storageNodes.remove((Short)bootService.getNodeID());
		// exclude MS compute master
		LOG.info(String.format("Exclude MS master node [%d]", stat.getMasterNodeId()));
		storageNodes.remove((Short)stat.getMasterNodeId());
		LOG.info(String.format("Slave nodes left for computation: %d", storageNodes.size()));

		LOG.info("Load graph data from storage to the memory space (chunks)");
		// TODO: job service is broken, first test BFS locally
		// load(jobService, storageNodes);
		LOG.info("Run the algorithm...");
		run();
		LOG.info("Remove graph data from the memory space (chunks)");
		// TODO: unload(jobService, storageNodes);
	}


}
