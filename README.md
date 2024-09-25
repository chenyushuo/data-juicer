[[中文主页]](README_ZH.md) | [[Docs]](#documents) | [[API]](https://modelscope.github.io/data-juicer) |  [[DJ-SORA]](docs/DJ_SORA.md) | [[Awesome List]](docs/awesome_llm_data.md)


# Data-Juicer

 <img src="https://img.alicdn.com/imgextra/i3/O1CN017Eq5kf27AlA2NUKef_!!6000000007757-0-tps-1280-720.jpg" width = "640" height = "360" alt="Data-Juicer"/>

![](https://img.shields.io/badge/language-Python-214870.svg)
![](https://img.shields.io/badge/license-Apache--2.0-000000.svg)
[![pypi version](https://img.shields.io/pypi/v/py-data-juicer?logo=pypi&color=026cad)](https://pypi.org/project/py-data-juicer)
[![Docker version](https://img.shields.io/docker/v/datajuicer/data-juicer?logo=docker&label=Docker&color=498bdf)](https://hub.docker.com/r/datajuicer/data-juicer)


Data-Juicer is a one-stop **multimodal** data processing system to make data higher-quality,
juicier, and more digestible for LLMs.



----


<div id="table" align="center"></div>

Table of Contents
=================

- [Data-Juicer:  A One-Stop Data Processing System for Large Language Models](#data-juicer--a-one-stop-data-processing-system-for-large-language-models)
  - [News](#news)
- [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Documentation Index ](#documentation-index-)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [From Source](#from-source)
    - [Using pip](#using-pip)
    - [Using Docker](#using-docker)
    - [Installation check](#installation-check)
  - [Quick Start](#quick-start)
    - [Data Processing](#data-processing)
    - [Distributed Data Processing](#distributed-data-processing)
    - [Data Analysis](#data-analysis)
    - [Data Visualization](#data-visualization)
    - [Build Up Config Files](#build-up-config-files)
    - [Sandbox](#sandbox)
    - [Preprocess Raw Data (Optional)](#preprocess-raw-data-optional)
    - [For Docker Users](#for-docker-users)
  - [Data Recipes](#data-recipes)
  - [License](#license)
  - [Contributing](#contributing)
  - [Acknowledgement](#acknowledgement)


## Features

![Overview](https://img.alicdn.com/imgextra/i4/O1CN01WYQP3Z1JHsaXaQDK6_!!6000000001004-0-tps-3640-1812.jpg)

- **Systematic & Reusable**:
  Empowering users with a systematic library of 80+ core [OPs](docs/Operators.md), 20+ reusable [config recipes](configs), and 20+ feature-rich
  dedicated [toolkits](#documentation), designed to
  function independently of specific multimodal LLM datasets and processing pipelines.

- **Data-in-the-loop & Sandbox**: Supporting one-stop data-model collaborative development, enabling rapid iteration
  through the [sandbox laboratory](docs/Sandbox.md), and providing features such as feedback loops based on data and model,
  visualization, and multidimensional automatic evaluation, so that you can better understand and improve your data and models.
  ![Data-in-the-loop](https://img.alicdn.com/imgextra/i2/O1CN017U7Zz31Y7XtCJ5GOz_!!6000000003012-0-tps-3640-1567.jpg)

- **Towards production environment **: Providing efficient and parallel data processing pipelines (Aliyun-PAI\Ray\Slurm\CUDA\OP Fusion)
  requiring less memory and CPU usage, optimized with automatic fault-toleration.
  ![sys-perf](https://img.alicdn.com/imgextra/i4/O1CN01Sk0q2U1hdRxbnQXFg_!!6000000004300-0-tps-2438-709.jpg)

- **Comprehensive Data Processing Recipes**: Offering tens of [pre-built data
  processing recipes](configs/data_juicer_recipes/README.md) for pre-training, fine-tuning, en, zh, and more scenarios. Validated on
  reference LLaMA and LLaVA models.
  ![exp_llama](https://img.alicdn.com/imgextra/i2/O1CN019WtUPP1uhebnDlPR8_!!6000000006069-2-tps-2530-1005.png)

- **Flexible & Extensible**: Accommodating most types of data formats (e.g., jsonl, parquet, csv, ...) and allowing flexible combinations of OPs. Feel free to [implement your own OPs](docs/DeveloperGuide.md#build-your-own-ops) for customizable data processing.

- **User-Friendly Experience**: Designed for simplicity, with [comprehensive documentation](#documents), [easy start guides](#quick-start) and [demo configs](configs/README.md), and intuitive configuration with simple adding/removing OPs from [existing configs](configs/config_all.yaml).



## Documentation Index <a name="documents"/>

- [Overview](README.md)
- [Operator Zoo](docs/Operators.md)
- [Configs](configs/README.md)
- [Developer Guide](docs/DeveloperGuide.md)
- ["Bad" Data Exhibition](docs/BadDataExhibition.md)
- Dedicated Toolkits
  - [Quality Classifier](tools/quality_classifier/README.md)
  - [Auto Evaluation](tools/evaluator/README.md)
  - [Preprocess](tools/preprocess/README.md)
  - [Postprocess](tools/postprocess/README.md)
- [Third-parties (LLM Ecosystems)](thirdparty/README.md)
- [API references](https://modelscope.github.io/data-juicer/)
- [Awesome LLM-Data](docs/awesome_llm_data.md)
- [DJ-SORA](docs/DJ_SORA.md)


## Prerequisites

- Recommend Python>=3.8,<=3.10
- gcc >= 5 (at least C++14 support)

## Installation

### From Source 

- Run the following commands to install the latest basic `data_juicer` version in
  editable mode:
```shell
cd <path_to_data_juicer>
pip install -v -e .
```

- Some OPs rely on some other too large or low-platform-compatibility third-party libraries. You can install optional dependencies as needed:

```shell
cd <path_to_data_juicer>
pip install -v -e .  # install a minimal dependencies, which support the basic functions
pip install -v -e .[tools] # install a subset of tools dependencies
```

The dependency options are listed below:

| Tag              | Description                                                                                  |
|------------------|----------------------------------------------------------------------------------------------|
| `.` or `.[mini]` | Install minimal dependencies for basic Data-Juicer.                                          |
| `.[all]`         | Install all dependencies except sandbox.                                                     |
| `.[sci]`         | Install all dependencies for all OPs.                                                        |
| `.[dist]`        | Install dependencies for distributed data processing. (Experimental)                         |
| `.[dev]`         | Install dependencies for developing the package as contributors.                             |
| `.[tools]`       | Install dependencies for dedicated tools, such as quality classifiers.                       |
| `.[sandbox]`     | Install all dependencies for sandbox.                                                        |

### Using pip

- Run the following command to install the latest released `data_juicer` using `pip`:

```shell
pip install py-data-juicer
```

- **Note**:
  - only the basic APIs in `data_juicer` and two basic tools
    (data [processing](#data-processing) and [analysis](#data-analysis)) are available in this way. If you want customizable
    and complete functions, we recommend you install `data_juicer` [from source](#from-source).
  - The release versions from pypi have a certain lag compared to the latest version from source.
    So if you want to follow the latest functions of `data_juicer`, we recommend you install [from source](#from-source).

### Using Docker

- You can
  - either pull our pre-built image from DockerHub:
    ```shell
    docker pull datajuicer/data-juicer:<version_tag>
    ```

  - or run the following command to build the docker image including the
    latest `data-juicer` with provided [Dockerfile](Dockerfile):

    ```shell
    docker build -t datajuicer/data-juicer:<version_tag> .
    ```

  - The format of `<version_tag>` is like `v0.2.0`, which is the same as release version tag.

### Installation check

```python
import data_juicer as dj
print(dj.__version__)
```

### For Video-related Operators
Before using video-related operators, **FFmpeg** should be installed and accessible via the $PATH environment variable.

You can install FFmpeg using package managers(e.g. sudo apt install ffmpeg on Debian/Ubuntu, brew install ffmpeg on OS X) or visit the [official ffmpeg link](https://ffmpeg.org/download.html).

Check if your environment path is set correctly by running the ffmpeg command from the terminal.


<p align="right"><a href="#table">🔼 back to index</a></p>


## Quick Start


### Data Processing

- Run `process_data.py` tool or `dj-process` command line tool with your config as the argument to process
  your dataset.

```shell
# only for installation from source
python tools/process_data.py --config configs/demo/process.yaml

# use command line tool
dj-process --config configs/demo/process.yaml
```

- **Note:** For some operators that involve third-party models or resources which are not stored locally on your computer, it might be slow for the first running because these ops need to download corresponding resources into a directory first.
The default download cache directory is `~/.cache/data_juicer`. Change the cache location by setting the shell environment variable, `DATA_JUICER_CACHE_HOME` to another directory, and you can also change `DATA_JUICER_MODELS_CACHE` or `DATA_JUICER_ASSETS_CACHE` in the same way:

- **Note:** When using operators with third-party models, it's necessary to declare the corresponding `mem_required` in the configuration file (you can refer to the settings in the `config_all.yaml` file). During runtime, Data-Juicer will control the number of processes based on memory availability and the memory requirements of the operator models to achieve better data processing efficiency. When running with CUDA environment, if the mem_required for an operator is not declared correctly, it could potentially lead to a CUDA Out of Memory issue.

```shell
# cache home
export DATA_JUICER_CACHE_HOME="/path/to/another/directory"
# cache models
export DATA_JUICER_MODELS_CACHE="/path/to/another/directory/models"
# cache assets
export DATA_JUICER_ASSETS_CACHE="/path/to/another/directory/assets"
```

#### Flexible Programming Interface
We provide various simple interfaces for users to choose from as follows. 
```python
#... init op & dataset ...

# Chain call style, support single operator or operator list
dataset = dataset.process(op)
dataset = dataset.process([op1, op2])
# Functional programming style for quick integration or script prototype iteration
dataset = op(dataset)
dataset = op.run(dataset)
```


### Distributed Data Processing

We have now implemented multi-machine distributed data processing based on [RAY](https://www.ray.io/). The corresponding demos can be run using the following commands:

```shell
# Run text data processing
python tools/process_data.py --config ./demos/process_on_ray/configs/demo.yaml
# Run video data processing
python tools/process_data.py --config ./demos/process_video_on_ray/configs/demo.yaml
```

- To run data processing across multiple machines, it is necessary to ensure that all distributed nodes can access the corresponding data paths (for example, by mounting the respective data paths on a file-sharing system such as NAS).
- The deduplicator operators for RAY mode are different from the single-machine version, and all those operators are prefixed with `ray`, e.g. `ray_video_deduplicator` and `ray_document_deduplicator`. Those operators also rely on a [Redis](https://redis.io/) instance. So in addition to starting the RAY cluster, you also need to setup your Redis instance in advance and provide `host` and `port` of your Redis instance in configuration.

> Users can also opt not to use RAY and instead split the dataset to run on a cluster with [Slurm](https://slurm.schedmd.com/) / [Aliyun PAI-DLC](https://www.aliyun.com/activity/bigdata/pai-dlc). In this case, please use the default Data-Juicer without RAY.


### Data Analysis
- Run `analyze_data.py` tool or `dj-analyze` command line tool with your config as the argument to analyze your dataset.

```shell
# only for installation from source
python tools/analyze_data.py --config configs/demo/analyzer.yaml

# use command line tool
dj-analyze --config configs/demo/analyzer.yaml
```

- **Note:** Analyzer only compute stats of Filter ops. So extra Mapper or Deduplicator ops will be ignored in the analysis process.

### Data Visualization

- Run `app.py` tool to visualize your dataset in your browser.
- **Note**: only available for installation from source.

```shell
streamlit run app.py
```

### Build Up Config Files

- Config files specify some global arguments, and an operator list for the
  data process. You need to set:
  - Global arguments: input/output dataset path, number of workers, etc.
  - Operator list: list operators with their arguments used to process the dataset.
- You can build up your own config files by:
  - ➖：Modify from our example config file [`config_all.yaml`](configs/config_all.yaml) which includes **all** ops and default
    arguments. You just need to **remove** ops that you won't use and refine
    some arguments of ops.
  - ➕：Build up your own config files **from scratch**. You can refer our
    example config file [`config_all.yaml`](configs/config_all.yaml), [op documents](docs/Operators.md), and advanced [Build-Up Guide for developers](docs/DeveloperGuide.md#build-your-own-configs).
  - Besides the yaml files, you also have the flexibility to specify just
    one (of several) parameters on the command line, which will override
    the values in yaml files.

```shell
python xxx.py --config configs/demo/process.yaml --language_id_score_filter.lang=en
```

- The basic config format and definition is shown below.

  ![Basic config example of format and definition](https://img.alicdn.com/imgextra/i1/O1CN01uXgjgj1khWKOigYww_!!6000000004715-0-tps-1745-871.jpg "Basic config file example")

### Sandbox

The data sandbox laboratory (DJ-Sandbox) provides users with the best practices for continuously producing data recipes. It features low overhead, portability, and guidance.

- In the sandbox, users can quickly experiment, iterate, and refine data recipes based on small-scale datasets and models, before scaling up to produce high-quality data to serve large-scale models.
- In addition to the basic data optimization and recipe refinement features offered by Data-Juicer, users can seamlessly use configurable components such as data probe and analysis, model training and evaluation, and data and model feedback-based recipe refinement to form a complete one-stop data-model research and development pipeline.

The sandbox is run using the following commands by default, and for more information and details, please refer to the [sandbox documentation](docs/Sandbox.md).
```shell
python tools/sandbox_starter.py --config configs/demo/sandbox/sandbox.yaml
```

### Preprocess Raw Data (Optional)
- Our formatters support some common input dataset formats for now:
  - Multi-sample in one file: jsonl/json, parquet, csv/tsv, etc.
  - Single-sample in one file: txt, code, docx, pdf, etc.
- However, data from different sources are complicated and diverse. Such as:
  - [Raw arXiv data downloaded from S3](https://info.arxiv.org/help/bulk_data_s3.html) include thousands of tar files and even more gzip files in them, and expected tex files are embedded in the gzip files so they are hard to obtain directly.
  - Some crawled data include different kinds of files (pdf, html, docx, etc.). And extra information like tables, charts, and so on is hard to extract.
- It's impossible to handle all kinds of data in Data-Juicer, issues/PRs are welcome to contribute to process new data types!
- Thus, we provide some **common preprocessing tools** in [`tools/preprocess`](tools/preprocess/) for you to preprocess these data.
  - You are welcome to make your contributions to new preprocessing tools for the community.
  - We **highly recommend** that complicated data can be preprocessed to jsonl or parquet files.

### For Docker Users

- If you build or pull the docker image of `data-juicer`, you can run the commands or tools mentioned above using this docker image.
- Run directly:

```shell
# run the data processing directly
docker run --rm \  # remove container after the processing
  --name dj \  # name of the container
  -v <host_data_path>:<image_data_path> \  # mount data or config directory into the container
  -v ~/.cache/:/root/.cache/ \  # mount the cache directory into the container to reuse caches and models (recommended)
  datajuicer/data-juicer:<version_tag> \  # image to run
  dj-process --config /path/to/config.yaml  # similar data processing commands
```

- Or enter into the running container and run commands in editable mode:

```shell
# start the container
docker run -dit \  # run the container in the background
  --rm \
  --name dj \
  -v <host_data_path>:<image_data_path> \
  -v ~/.cache/:/root/.cache/ \
  datajuicer/data-juicer:latest /bin/bash

# enter into this container and then you can use data-juicer in editable mode
docker exec -it <container_id> bash
```


<p align="right"><a href="#table">🔼 back to index</a></p>

## Data Recipes
- [Recipes for data process in BLOOM](configs/reproduced_bloom/README.md)
- [Recipes for data process in RedPajama](configs/redpajama/README.md)
- [Refined recipes for pre-training text data](configs/data_juicer_recipes/README.md)
- [Refined recipes for fine-tuning text data](configs/data_juicer_recipes/README.md#before-and-after-refining-for-alpaca-cot-dataset)
- [Refined recipes for pre-training multi-modal data](configs/data_juicer_recipes/README.md#before-and-after-refining-for-multimodal-dataset)



## License
Data-Juicer is released under Apache License 2.0.

## Contributing
We are in a rapidly developing field and greatly welcome contributions of new
features, bug fixes and better documentations. Please refer to
[How-to Guide for Developers](docs/DeveloperGuide.md).

If you have any questions, please join our [discussion groups](README.md).

## Acknowledgement
Data-Juicer is used across various LLM products and research initiatives,
including industrial LLMs from Alibaba Cloud's Tongyi, such as Dianjin for
financial analysis, and Zhiwen for reading assistant, as well as the Alibaba
Cloud's platform for AI (PAI).
We look forward to more of your experience, suggestions and discussions for collaboration!

Data-Juicer thanks and refers to several community projects, such as
[Huggingface-Datasets](https://github.com/huggingface/datasets), [Bloom](https://huggingface.co/bigscience/bloom), [RedPajama](https://github.com/togethercomputer/RedPajama-Data/tree/rp_v1), [Pile](https://huggingface.co/datasets/EleutherAI/pile), [Alpaca-Cot](https://huggingface.co/datasets/QingyiSi/Alpaca-CoT), [Megatron-LM](https://github.com/NVIDIA/Megatron-LM), [DeepSpeed](https://www.deepspeed.ai/), [Arrow](https://github.com/apache/arrow), [Ray](https://github.com/ray-project/ray), [Beam](https://github.com/apache/beam),  [LM-Harness](https://github.com/EleutherAI/lm-evaluation-harness), [HELM](https://github.com/stanford-crfm/helm), ....


<p align="right"><a href="#table">🔼 back to index</a></p>
