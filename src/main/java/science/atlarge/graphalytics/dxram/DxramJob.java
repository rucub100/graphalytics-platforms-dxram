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
package science.atlarge.graphalytics.dxram;

import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;

import java.io.IOException;
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
			.toString();;

		this.platformConfig = platformConfig;
	}

	protected abstract void run() throws Exception;

	/**
	 * Executes the platform job with the pre-defined parameters.
	 *
	 * @return the exit code
	 * @throws IOException if the platform failed to run
	 */
	protected void execute() throws Exception {
		LOG.info("Execute benchmark job");

		BootService bootService = getService(BootService.class);
		JobService jobService = getService(JobService.class);

		// get a list of all nodes for storage and computations
		List<Short> storageAndComputeNodes = bootService
				.getSupportingNodes(NodeCapabilities.STORAGE | NodeCapabilities.COMPUTE);
		// exclude this node which controls the benchmark and coordinates the runs
		storageAndComputeNodes.remove(bootService.getNodeID());

		load(jobService, storageAndComputeNodes);
		run();
		unload(jobService, storageAndComputeNodes);
	}

	private void unload(JobService jobService, List<Short> storageAndComputeNodes) {
		// TODO Auto-generated method stub
		// push remote job to all storage nodes to drop all chunks

		jobService.waitForAllJobsToFinish();
	}

	private void load(JobService jobService, List<Short> storageAndComputeNodes) {
		// TODO Auto-generated method stub
		/**
		 * Push remote job to all storage nodes for loading the graph into the chunk storage
		 * First load whole graph on each node, later consider to partition/load only the relevant part?!
		 */

		LoadGraphJob loadGraphJob = new LoadGraphJob();

		for (Short nodeId : storageAndComputeNodes) {
			jobService.pushJobRemote(loadGraphJob, nodeId);
		}

		jobService.waitForAllJobsToFinish();
	}
}
