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
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderException;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJVMArgs;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJsonFile2;
import de.hhu.bsinfo.dxram.job.JobID;
import de.hhu.bsinfo.dxram.job.JobService;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.domain.graph.LoadedGraph;
import science.atlarge.graphalytics.execution.Platform;
import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.execution.BenchmarkRunner;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.report.result.BenchmarkMetric;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;
import science.atlarge.graphalytics.dxram.algorithms.AlgorithmJobFactory;
import science.atlarge.graphalytics.dxram.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.dxram.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.dxram.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.dxram.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.dxram.algorithms.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.dxram.algorithms.wcc.WeaklyConnectedComponentsJob;
import science.atlarge.graphalytics.dxram.job.DropAllChunksJob;
import science.atlarge.graphalytics.dxram.job.DxramJob;
import science.atlarge.graphalytics.dxram.job.GraphalyticsAbstractJob;
import science.atlarge.graphalytics.dxram.job.LoadGraphJob;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Dxram platform driver for the Graphalytics benchmark.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 30, 2018
 */
public class DxramPlatform implements Platform {

    private static DXRAM dxram;
    private static JobService jobService;
    private static long MAKESPAN_START = 0;
    private static long MAKESPAN_END = 0;

    protected static final Logger LOG = LogManager.getLogger();

    public static final String PLATFORM_NAME = "dxram";

    public DxramPlatform() {
    }

    @Override
    public void verifySetup() throws Exception {
    }

    @Override
    public LoadedGraph loadGraph(FormattedGraph formattedGraph) throws Exception {
        String edgeFilePath = formattedGraph.getEdgeFilePath();
        if (!formattedGraph.isDirected()) {
            edgeFilePath += ".2";
            final Map<Long, SortedSet<Long>> tmpEdges = new HashMap<Long, SortedSet<Long>>();

            try (
                    final BufferedWriter bw = Files.newBufferedWriter(
                            Paths.get(edgeFilePath), StandardCharsets.US_ASCII,
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    final BufferedReader br = Files.newBufferedReader(
                            Paths.get(formattedGraph.getEdgeFilePath()),
                            StandardCharsets.US_ASCII)) {
                String line = br.readLine();
                while (line != null) {
                    String[] tmp = line.split("\\s");
                    final long left = Long.parseLong(tmp[0]);
                    final long right = Long.parseLong(tmp[1]);
                    // TODO(later): handle properties payload, i.e. if tmp.length > 2
                    
                    if (!tmpEdges.containsKey(left)) {
                        tmpEdges.put(left, new TreeSet<Long>());
                    }
                    if (!tmpEdges.containsKey(right)) {
                        tmpEdges.put(right, new TreeSet<Long>());
                    }

                    tmpEdges.get(left).add(right);
                    tmpEdges.get(right).add(left);
                    line = br.readLine();
                }

                final LinkedList<Long> sortedLeft = new LinkedList<Long>(tmpEdges.keySet());
                Collections.sort(sortedLeft);

                while (!sortedLeft.isEmpty()) {
                    final long left = sortedLeft.poll();
                    for (long right : tmpEdges.remove(left)) {
                        bw.write(String.valueOf(left) + " " + String.valueOf(right));
                        bw.write('\n');
                    }
                }
            } catch (Exception e) {
                throw new PlatformExecutionException("Failed to load the undirected graph!", e);
            }
        }

        return new LoadedGraph(formattedGraph, formattedGraph.getVertexFilePath(), edgeFilePath);
    }

    @Override
    public void deleteGraph(LoadedGraph loadedGraph) throws Exception {
    }

    @Override
    public void prepare(RunSpecification runSpecification) throws Exception {
        // create graph partition index file
        final String gpiPath = runSpecification.getRuntimeSetup().getLoadedGraph().getVertexPath() + ".gpi";
        final long cntVertices = runSpecification.getBenchmarkRun().getFormattedGraph().getNumberOfVertices();
        final long cntEdges = runSpecification.getBenchmarkRun().getFormattedGraph().getNumberOfEdges();

        if (Files.exists(Paths.get(gpiPath))) {
            return;
        }

        try (BufferedWriter bw = Files.newBufferedWriter(
                Paths.get(gpiPath), StandardCharsets.US_ASCII,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            bw.write("0, " + String.valueOf(cntVertices) + ", " + String.valueOf(cntEdges) + ", " + "0");
        }
    }

    @Override
    public void startup(RunSpecification runSpecification) throws Exception {
        MAKESPAN_START = System.currentTimeMillis();
        DxramCollector.startPlatformLogging(
                runSpecification.getBenchmarkRunSetup().getLogDir().resolve("platform").resolve("runner.logs"));
        initDxram();
        registerJobTypes();
    }

    @Override
    public void run(RunSpecification runSpecification) throws PlatformExecutionException {
        BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
        DxramConfiguration platformConfig = DxramConfiguration.parsePropertiesFile();

        DxramJob job = AlgorithmJobFactory.newJob(runSpecification, platformConfig);

        LOG.info("Executing benchmark with algorithm \"{}\" on graph \"{}\".", benchmarkRun.getAlgorithm().getName(),
                benchmarkRun.getFormattedGraph().getName());

        try {
            if (jobService.pushJob(job) == JobID.INVALID_ID) {
                throw new Exception(String.format("Invalid job id for %s", job));
            }

            if (!jobService.waitForLocalJobsToFinish()) {
                throw new Exception("Failed waiting for local jobs to finish.");
            }
            // TODO if job.execException != null => throw execException
        } catch (Exception e) {
            throw new PlatformExecutionException("Failed to execute a Dxram job.", e);
        }

        LOG.info("Executed benchmark with algorithm \"{}\" on graph \"{}\".", benchmarkRun.getAlgorithm().getName(),
                benchmarkRun.getFormattedGraph().getName());
    }

    @Override
    public BenchmarkMetrics finalize(RunSpecification runSpecification) throws Exception {
        MAKESPAN_END = System.currentTimeMillis();
        dxram.shutdown();
        DxramCollector.stopPlatformLogging();
        BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
        Path logDir = benchmarkRunSetup.getLogDir().resolve("platform");

        BenchmarkMetrics metrics = new BenchmarkMetrics();
        metrics.setProcessingTime(DxramCollector.collectProcessingTime(logDir));
        metrics.setMakespan(new BenchmarkMetric(new BigDecimal(MAKESPAN_END - MAKESPAN_START), "ms"));
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
