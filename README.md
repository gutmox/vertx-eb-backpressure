# vertx Event Bus Backpressure

Vertx Proof of Concept to control backpressure to Redis using the event bus. 

## Building and running using Bazel

```
bazel run :service
```

## Passing unit tests using Bazel

```
bazel test :tests
```  

## Running from IntelliJ

![IntelliJ](https://github.com/gutmox/vertx-boost/blob/master/doc/intellij.png?raw=true)  

### Building docker container

```
bazel build :container
```
#### Publishing docker image

```
bazel run :container
```

### Generating fat jar

```
bazel build :run_deploy.jar
```

