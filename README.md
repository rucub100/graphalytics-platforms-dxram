# Graphalytics Dxram platform driver

DXRAM is a distributed in-memory key-value store for low-latency cloud applications, e.g. social networks, search engines and I/O-bound long running scientific computations running in a data center. It is designed to efficiently manage billions of small data objects which are typical for graph based applications. To execute Graphalytics benchmark on Dxram, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the Dxram-specific instructions listed below.

### Obtain the platform driver
There is currently only one possible way to obtain the Dxram platform driver:

 1. **Build the platform driver**: 
  - Download the source code from this repository.
  - Execute `mvn clean package` in the root directory (See details in [Software Build](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation:-Software-Build)).
  - Extract the distribution from  `graphalytics-{graphalytics-version}-dxram-{platform-version}.tar.gz`.
  - Copy the dxram distribution into the Graphalytics distribution

### Verify the necessary prerequisites
The softwares listed below are required by the Dxram platform driver, which should be properly configured in the cluster environment: See [DXRAM repository](https://github.com/hhu-bsinfo/dxram).

### Adjust the benchmark configurations
Adjust the Dxram configurations in `config/platform.properties` and `config/dxram.json`.
