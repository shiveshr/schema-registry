<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

`PoC for a lightweight functions framework for pravega`.
Note: this is not a proposal but a PoC of one of the ways of thinking of higher level of processing abstraction of pravega
streams rather than working directly with pravega writers and readers and can help avoid a lot of boiler plate code for 
processing data in pravega streams. 

Typical processing mode on streaming data is `read-process-write` or `consume-transform-produce`. A pipeline of multiple 
`read-process-write` processing can be used to describe a full processing flow where data is read from one stream, goes through
multiple transformation and optionally published to multiple intermediate streams before its final output is published as a 
continuous stream of result into an output pravega stream. 

The motivation for functions framework is to provide a lightweight compute environment with higher level of abstraction 
for working with pravega streams. 

Functions are the middle(transform/process) block in `consume-transform-produce`/`read-process-write` model of processing of data in streams.

Conceptualizing such a processing framework will constitute of three important pillars. 
1. A central repository of function artifacts (jars).
2. An express for a processing pipeline on pravega streams which apply one of more functions on the data in the stream 
and optionally publish the output into another stream. 
3. A function `Runtime` - that is responsible for executing the processing pipeline. Runtime could be created in multiple
flavours - a) run within user process b) deploys containers using some container cluster manager like k8s, docker swarm etc. 

1. Function [interface] (https://github.com/shiveshr/schema-registry/tree/functions/samples/src/main/java/io/pravega/schemaregistry/test/integrationtest/demo/function/interfaces) 
A function is a simple interface just like a java function. 
```
public interface Function<Input, Output> {
    Output apply(Input i);
}
```
And another Windowed function as:
```
public interface WindowedFunction<Input, Output> {
    Output apply(Collection<Input> i);
}
```

Users can provide implementations of either of these functions and publish these functions into a centralized functions repository. 
Draw an analogy of functions to User Defined Functions in the databases. 
One can create the function once, and publish it in a central functions respository. Then this function 
can be referenced in any data pipeline. 
These functions can be thought of as pure functions and multiple functions can be composed as f(g(x)) etc.

2. [Stream processing] (https://github.com/shiveshr/schema-registry/blob/functions/samples/src/main/java/io/pravega/schemaregistry/test/integrationtest/demo/function/runtime/StreamProcess.java)
A mechanism to describe data processing pipeline. Where you start with one stream to read from and end with one stream to write the computation result into.
There are bunch of intermediate transforms or filters that are basically `Functions` that are chained one after the other in order. 
The computations can be described locally as transformations (java functions) or by referencing an external user defined function. 
The referenced user defined function will be retrieved from central repository and plugged into the pipeline at runtime. 

We can expand this into a DSL which will be parsed and translated into a stream processing pipeline. 

3. [Function Runtime] (https://github.com/shiveshr/schema-registry/blob/functions/samples/src/main/java/io/pravega/schemaregistry/test/integrationtest/demo/function/runtime/Runtime.java)
Function runtime takes a `stream processing` job as input and deploys pravega readers and writers and execution contexts where it
reads events and applies series of functions on them and then produces the output into another pravega stream. 

There could be different flavors of Runtime ranging from in-process execution (via a library that users may use in their application) 
to a processing framework that deploys different steps of processing pipeline into different pods and tracks their progress. 

Demo:  
For this sample, start with `FunctionsDemo.java` which demonstrates a `windowed` word count example.
A stream processing pipeline is described in following way:
```
        StreamProcess<MyInput, Map<String, Integer>> process = new StreamProcess.StreamProcessBuilder<MyInput, Map<String, Integer>>()
                .inputStream(scope, inputStream, inputSerDe, serDeFilepath)
                .map(x -> ((MyInput) x).getText())
                .map(toLowerFunc, funcFilepath)
                .map(x -> ((String) x).split("\\W+"))
                .windowedMap(wordCountFunc, funcFilepath, 2)
                .outputStream(scope, outputStream, outputSerDe, serDeFilepath)
                .build();               
```        
Here `toLowerFunc` and `wordCountFunc` are two user defined functions that "would be" loaded at runtime from a central functions
repository.

A pipeline of multiple such processes can be created where output of one streamprocess being written into a pravega stream becomes input for another StreamProcess in the pipeline. 
Once pipeline is described, it will be presented to a runtime to plan and execute it.   
```
        Pipeline<MyInput, Map<String, Integer>> processPipeline = Pipeline.of(process);
        Runtime runtime = new Runtime(clientConfig, srClient, processPipeline);
```

This demonstrates three important points -
1. expressing processing using higher level abstractions rather than working with low level pravega readers and writers. 
2. External Functions can be referenced in the pipeline and dynamically loaded at execution time. 
3. A runtime to execute the processing pipeline in different evironments.  

 
