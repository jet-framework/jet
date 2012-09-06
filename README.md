### Jet: An Embedded DSL for Distributed Data Parallel Processing

Jet is a multy-stage programming approach for distributed data parallel processing.
It is embedded in Scala, has a declarative and high level programming model similar to the [Spark](http://spark-project.org/) framework. 
However, due to the multi-stage programming approach, Jet does the following optimizations:
  * Inserts projection before barriers to remove unused fields of Scala case classes.
  * Move pure expressions (e.g. `Date`, `java.util.regex.Pattern`) out of the hot path
  * Fuse all `flatMap`, `map` and `filter` and apply other optimizations across them

From the same programming model, Stivo can generate code for [Spark](http://spark-project.org/), [Scoobi](https://github.com/NICTA/scoobi) and to [Crunch](https://github.com/cloudera/crunch). 

NOTE: Stivo is still in the experimental phase of development. It can not be run on Hadoop and Spark out of the box. Before making it production ready we plan to introduce more relational optimizations (predicate pushdown, cost-based join reordering etc.) and make it interoperable with other DSLs like Regular Expressions DSL.

A paper on Jet has been published at the BigData '12 workshop and will appear in the proceedings of the conference.

####Folder layout:
* dsl contains the implementation of the DSL
* virtualization-lms-core is the underlying framework, which deals with code motion and 
many other aspects of a compiler
* scoobi is the target for generated sources for scoobi
* spark is the target for generated sources for spark
* crunch is the target for generated sources for crunch
* pig contains the pig scripts we use as reference
* benchmarking contains some scripts to help with benchmarking
* utils contains small utilities
* project contains the sbt configuration

This library is not (yet) meant for public usage. It is more of a prototype than a correct implementation.

####Getting started:
The framework depends on the customized version of Lightweight Modular Staging framework. To initialize it invoke:

    mkdir virtualization-lms-core
    git submodule init
    git submodule update

For building you will need:
* sbt 0.11.3 launcher
* git

To get started, run setup.sh. This will fetch spark and deploy it to the local repository. Make sure that spark has been deployed correctly.
When this is done, you can start sbt in the top distributed folder. For generating the classes you can use:
	~ ; dsl/test ; dotgen ; gens/compile
This will regenerate all test classes, update the utils, generate dot files and compile the generated files.

For benchmarking and packaging, I use the scripts in benchmarking. They are pretty messy though. Install Cloudera cdh3u4 on your machine to test the generated Crunch and Scoobi programs.

####Writing a program:
See the examples in dsl/src/test/scala. To import your own class, create it in the dsl/structs folder, following the examples. Then use the dsl/startLift.sh script to refresh ApplicationOps, which you can then mix into your application.
