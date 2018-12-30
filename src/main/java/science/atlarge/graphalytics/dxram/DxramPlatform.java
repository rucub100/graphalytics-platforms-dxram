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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderException;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJVMArgs;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJsonFile2;
import de.hhu.bsinfo.dxram.job.JobService;
import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.domain.graph.LoadedGraph;
import science.atlarge.graphalytics.execution.Platform;
import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.execution.BenchmarkRunner;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.execution.RuntimeSetup;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;
import science.atlarge.graphalytics.dxram.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.dxram.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.dxram.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.dxram.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.dxram.algorithms.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.dxram.algorithms.wcc.WeaklyConnectedComponentsJob;
import java.util.List;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Path;

/**
 * Dxram platform driver for the Graphalytics benchmark.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 30, 2018
 */
public class DxramPlatform implements Platform {

	private static DXRAM dxram;
	private static BootService bootService;
	private static ChunkService chunkService;
	private static JobService jobService;

	protected static final Logger LOG = LogManager.getLogger();

	public static final String PLATFORM_NAME = "dxram";

	public DxramPlatform() {}

	@Override
	public void verifySetup() throws Exception {}

	@Override
	public LoadedGraph loadGraph(FormattedGraph formattedGraph) throws Exception {
		return new LoadedGraph(
				formattedGraph,
				formattedGraph.getVertexFilePath(),
				formattedGraph.getEdgeFilePath());
	}

	@Override
	public void deleteGraph(LoadedGraph loadedGraph) throws Exception {}

	@Override
	public void prepare(RunSpecification runSpecification) throws Exception {}

	@Override
	public void startup(RunSpecification runSpecification) throws Exception {
		DxramCollector.startPlatformLogging(
				runSpecification
					.getBenchmarkRunSetup()
					.getLogDir()
					.resolve("platform")
					.resolve("runner.logs"));
		initDxram();
		registerJobTypes();
	}

	@Override
	public void run(RunSpecification runSpecification) throws PlatformExecutionException {
		BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();

		Algorithm algorithm = benchmarkRun.getAlgorithm();
		DxramConfiguration platformConfig = DxramConfiguration.parsePropertiesFile();

		DxramJob job;
		switch (algorithm) {
		case BFS:
			job = new BreadthFirstSearchJob(runSpecification, platformConfig);
			jobService.pushJob((BreadthFirstSearchJob)job);
			break;
		case CDLP:
			job = new CommunityDetectionLPJob(runSpecification, platformConfig);
			jobService.pushJob((CommunityDetectionLPJob)job);
			break;
		case LCC:
			job = new LocalClusteringCoefficientJob(runSpecification, platformConfig);
			jobService.pushJob((LocalClusteringCoefficientJob)job);
			break;
		case PR:
			job = new PageRankJob(runSpecification, platformConfig);
			jobService.pushJob((PageRankJob)job);
			break;
		case WCC:
			job = new WeaklyConnectedComponentsJob(runSpecification, platformConfig);
			jobService.pushJob((WeaklyConnectedComponentsJob)job);
			break;
		case SSSP:
			job = new SingleSourceShortestPathsJob(runSpecification, platformConfig);
			jobService.pushJob((SingleSourceShortestPathsJob)job);
			break;
		default:
			throw new PlatformExecutionException("Failed to load algorithm implementation.");
		}

		LOG.info(
				"Executing benchmark with algorithm \"{}\" on graph \"{}\".",
				benchmarkRun.getAlgorithm().getName(),
				benchmarkRun.getFormattedGraph().getName());

		try {
			// TODO: this does not throw any exceptions, design an alternative approach
			jobService.waitForLocalJobsToFinish();
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to execute a Dxram job.", e);
		}

		LOG.info(
				"Executed benchmark with algorithm \"{}\" on graph \"{}\".",
				benchmarkRun.getAlgorithm().getName(),
				benchmarkRun.getFormattedGraph().getName());
	}

	@Override
	public BenchmarkMetrics finalize(RunSpecification runSpecification) throws Exception {
		dxram.shutdown();
		DxramCollector.stopPlatformLogging();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		Path logDir = benchmarkRunSetup.getLogDir().resolve("platform");

		BenchmarkMetrics metrics = new BenchmarkMetrics();
		metrics.setProcessingTime(DxramCollector.collectProcessingTime(logDir));
		return metrics;
	}

	@Override
	public void terminate(RunSpecification runSpecification) throws Exception {
		BenchmarkRunner.terminatePlatform(runSpecification);
	}

	@Override
	public String getPlatformName() {
		return PLATFORM_NAME;
	}

	/**
	 * Initializes this driver's DXRAM instance.
	 */
	private void initDxram() {
		printJVMArgs();
		System.out.println();

		dxram = new DXRAM();

		System.out.println("Starting DXRAM, version " + dxram.getVersion());

		DXRAMConfig config = bootstrapConfig(dxram);

		if (!dxram.initialize(config, true)) {
			System.out.println("Initializing DXRAM failed.");
			System.exit(-1); // TODO: might be better to throw an exception here?
		}

		bootService = dxram.getService(BootService.class);
		chunkService = dxram.getService(ChunkService.class);
		jobService = dxram.getService(JobService.class);
	}

	/**
	 * Register all job types.
	 */
	private void registerJobTypes() {
		// abstract types
		jobService.registerJobType(GraphalyticsAbstractJob.TYPE_ID, GraphalyticsAbstractJob.class);
		jobService.registerJobType(DxramJob.TYPE_ID, DxramJob.class);

		// load and unload
		jobService.registerJobType(LoadGraphJob.TYPE_ID, LoadGraphJob.class);
		jobService.registerJobType(DropAllChunksJob.TYPE_ID, DropAllChunksJob.class);

		// algorithms
		jobService.registerJobType(BreadthFirstSearchJob.TYPE_ID, BreadthFirstSearchJob.class);
		jobService.registerJobType(CommunityDetectionLPJob.TYPE_ID, CommunityDetectionLPJob.class);
		jobService.registerJobType(LocalClusteringCoefficientJob.TYPE_ID, LocalClusteringCoefficientJob.class);
		jobService.registerJobType(PageRankJob.TYPE_ID, PageRankJob.class);
		jobService.registerJobType(SingleSourceShortestPathsJob.TYPE_ID, SingleSourceShortestPathsJob.class);
		jobService.registerJobType(WeaklyConnectedComponentsJob.TYPE_ID, WeaklyConnectedComponentsJob.class);
	}

	/**
	 * Bootstrap configuration loading/creation
	 *
	 * @param p_dxram DXRAM instance
	 * @return Configuration instance to use to initialize DXRAM
	 */
	private static DXRAMConfig bootstrapConfig(final DXRAM p_dxram) {
		DXRAMConfig config = p_dxram.createDefaultConfigInstance();

		DXRAMConfigBuilderJsonFile2 configBuilderFile = new DXRAMConfigBuilderJsonFile2();
		DXRAMConfigBuilderJVMArgs configBuilderJvmArgs = new DXRAMConfigBuilderJVMArgs();

		// JVM args override any default and/or config values loaded from file
		try {
			config = configBuilderJvmArgs.build(configBuilderFile.build(config));
		} catch (final DXRAMConfigBuilderException e) {
			System.out.println("Bootstrapping configuration failed: " + e.getMessage());
			System.exit(-1);
		}

		return config;
	}

	/**
	 * Print all JVM args specified on startup
	 */
	private static void printJVMArgs() {
		RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
		List<String> args = runtimeMxBean.getInputArguments();

		StringBuilder builder = new StringBuilder();
		builder.append("JVM arguments: ");

		for (String arg : args) {
			builder.append(arg);
			builder.append(' ');
		}

		System.out.println(builder);
		System.out.println();
	}
}
