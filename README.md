# HKube Python Wrapper
[![Build Status](https://travis-ci.org/kube-HPC/python-wrapper.hkube.svg?branch=master)](https://travis-ci.org/kube-HPC/python-wrapper.hkube)

Hkube python wrapper provides a simple interface for integrating algorithm in HKube

For general information on HKube see [hkube.io](http://hkube.io/)
## Installation
```shell
pip install hkube-python-wrapper
```
## Basic Usage
```python
from hkube_python_wrapper import Algorunner
def start(args, hkubeApi):
    return 1
if __name__ == "__main__":
    Algorunner.Run(start=start)
```

The `start` method accepts two arguments: 

**args**: dict of invocation input
| key            | type   | description                                                   |
|----------------|--------|---------------------------------------------------------------|
| input          | Array  | algorithm input as defined in the pipeline descriptor         |
| jobId          | string | The job ID of the pipeline run                                |
| taskId         | string | The task ID of the algorithm invocation                       |
| nodeName       | string | The name of the node in the pipeline descriptor               |
| pipelineName   | string | The name of the pipeline                                      |
| batchIndex     | int    | For batch instance, the index in the batch array              |
| parentNodeName | string | For child (code-api) algorithm. The name of the invoking node |
| info.rootJobId | string | for sub-pipeline, the jobId of the invoking pipeline          |
**hkubeApi**: instance of HKubeApi for code-api operations


## Class HKubeApi
---

Method `start_algorithm`
----
>     def start_algorithm(
>         self,
>         algorithmName,
>         input=[],
>         includeResult=True,
>         blocking=False
>     )
Starts an invocation of algorithm with input, and optionally waits for results

#### Args
**```algorithmName```**: `string`
:   The name of the algorithm to start.

**```input```** :`array`
:   Optional input for the algorithm.

**```includeResult```** :`bool`
:   if True, returns the result of the algorithm execution.  
    default: True
    
**```blocking```** :`bool`
:   if True, blocks until the algorithm finises, and returns the results.
    If False, returns an awaiter object, that can be awaited (blocking) at a later time  
    default: False
    
#### Returns
if blocking==False, returns an awaiter. If true, returns the result of the algorithm

#### Example:
```python
hkubeApi.start_algorithm('some_algorithm',input=[3], blocking=True)
```

Method `start_stored_subpipeline`
----
>     def start_stored_subpipeline(
>         self,
>         name,
>         flowInput={},
>         includeResult=True,
>         blocking=False
>     )
Starts an invocation of a sub-pipeline with input, and optionally waits for results 

#### Args
**```name```** : `string` 
:   The name of the pipeline to start.


**```flowInput```** : `dict`
:   Optional flowInput for the pipeline.


**```includeResult```** :`bool`
:   if True, returns the result of the pipeline execution.  
    default: True


**```blocking```** :&ensp;<code>bool</code>
:   if True, blocks until the pipeline finises, and returns the results.
    If False, returns an awaiter object, that can be awaited (blocking) at a later time  
    default: False


#### Returns
if blocking==False, returns an awaiter. If true, returns the result of the pipeline

#### Example:
```python
hkubeApi.start_stored_subpipeline('simple',flowInput={'foo':3},blocking=True)
```

Method `start_raw_subpipeline`
----
>     def start_raw_subpipeline(
>         self,
>         name,
>         nodes,
>         flowInput,
>         options={},
>         webhooks={},
>         includeResult=True,
>         blocking=False
>     )
Starts an invocation of a sub-pipeline with input, nodes, options, and optionally waits for results 

#### Args
**```name```** : `string` 
:   The name of the pipeline to start.

**```nodes```** : `string` 
:   Array of nodes. See example below.

**```flowInput```** : `dict`
:   FlowInput for the pipeline.

**```options```** : `dict`
:   pipeline options (like in the pipeline descriptor).

**```webhooks```** : `dict`
:   webhook options (like in the pipeline descriptor).

**```includeResult```** :`bool`
:   if True, returns the result of the pipeline execution.  
    default: True

**```blocking```** :&ensp;<code>bool</code>
:   if True, blocks until the pipeline finises, and returns the results.
    If False, returns an awaiter object, that can be awaited (blocking) at a later time  
    default: False

#### Returns
if blocking==False, returns an awaiter. If true, returns the result of the pipeline

#### Example:
```python
nodes=[{'nodeName': 'd1', 'algorithmName': 'green-alg', 'input': ['@flowInput.foo']}]
flowInput={'foo':3}
hkubeApi.start_raw_subpipeline('ddd',nodes, flowInput,webhooks={}, options={}, blocking=True)
```
