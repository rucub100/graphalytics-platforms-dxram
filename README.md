# Graphalytics Dxram platform driver

Dxram... (TODO: provide a short description on Dxram). To execute Graphalytics benchmark on Dxram, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the Dxram-specific instructions listed below.

### Obtain the platform driver
There are two possible ways to obtain the Dxram platform driver:

 1. **Download the (prebuilt) [Dxram platform driver](http://graphalytics.site/dist/stable/) distribution from our website.

 2. **Build the platform drivers**: 
  - Download the source code from this repository.
  - Execute `mvn clean package` in the root directory (See details in [Software Build](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation:-Software-Build)).
  - Extract the distribution from  `graphalytics-{graphalytics-version}-dxram-{platform-version}.tar.gz`.

### Verify the necessary prerequisites
The softwares listed below are required by the Dxram platform driver, which should be properly configured in the cluster environment....

### Adjust the benchmark configurations
Adjust the Dxram configurations in `config/platform.properties`...

