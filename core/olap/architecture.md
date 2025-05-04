### OlapParallelContext and Plugin System# Core/OLAP Component Architecture

## Overview

The `core/olap` component of Proteus is a high-performance analytics engine built on top of the core/codegen JIT compilation framework. It provides a scalable and efficient execution environment for analytical queries on heterogeneous hardware including multi-core CPUs and GPUs. The OLAP component implements both tuple-at-a-time processing and packed (vectorized) processing, enabling flexible data parallelism through its routing facilities.

## Key Components

### Operator Interface

The foundation of the OLAP component is its operator-based execution model:

1. **Core Methods**
    - Defined in `operators.hpp`
    - Each operator implements two key methods:
        - `produce_(OlapParallelContext *context)`: Generates code to produce data
        - `consume(Context *const context, const OperatorState &childState)`: Generates code to consume data
    - These methods are used during code generation, not data processing execution
    - The `produce()` final method is a wrapper around `produce_()` that ensures proper behavior
    - Operators also provide metadata through methods like:
        - `getRowType()`: Returns the schema of rows produced by the operator
        - `isPacked()`: Indicates whether the operator works with blocks or tuples
        - `isFiltering()`: Indicates whether the operator filters data
        - `getDeviceType()`, `getDOP()`: Indicate target device and degree of parallelism

2. **Operator Types**
    - `Operator`: Abstract base class for all operators
    - `UnaryOperator`: Operators with a single child (e.g., `Select`, `Project`)
    - `BinaryOperator`: Operators with two children (e.g., `Join`)
    - Common operators include: `Select`, `BlockToTuples`, `Router`, joins, aggregations
    - Device-specific operators: CPU operators, GPU-optimized operators, and cross-device operators

### Code Generation Process

The OLAP component uses a sophisticated code generation process to create efficient execution pipelines:

1. **Compilation Initiation**
    - The `prepare()` method of `RelBuilder` starts code generation:
        - Creates an `OlapParallelContext` for the query
        - Calls `produce_()` on the root operator to begin code generation

2. **Top-down Phase (Produce Phase)**
    - Starting from the root, each operator's `produce_()` method is called
    - Operators typically call `produce_()` on their children before generating their own code
    - This creates a recursive top-down traversal until reaching leaf operators
    - Leaf operators (like `Scan`) begin the actual code generation process

3. **Bottom-up Phase (Consume Phase)**
    - After a child operator generates its code, it calls its parent's `consume()` method
    - The parent generates code to process data coming from the child
    - This creates a recursive bottom-up code generation flow until reaching the root

4. **Pipeline Generation**
    - The result is a series of pipeline functions in LLVM IR
    - Each *compiled pipeline* corresponds to a segment of the logical operator tree between code generation breakers
    - Operators between code generation breakers are fused into tight loops in the generated code
    - These pipelines are compiled to machine code and wrapped in `Pipeline` objects
    - A classical operator pipeline (i.e. pipeline in the literature) may consist of multiple compiled pipelines
    - The resulting `PreparedStatement` manages these `Pipeline` objects for execution

### Pipeline Breakers and Pipeline System

The OLAP system distinguishes between two types of pipeline breakers that affect code generation and execution:

1. **Full Pipeline Breakers (Classical Pipeline Breakers)**
    - Operators like hash joins that require full materialization of input
    - Example: A hash join's build side must be fully materialized before the probe side can proceed
    - These create distinct logical operator pipelines that execute in separate phases
    - Full pipeline breakers enforce data dependencies and execution ordering

2. **Code Generation Breakers**
    - Operators like `Router` and `MemMove` that need to materialize intermediate results
    - A single logical operator pipeline may consist of multiple compiled pipelines
    - Example: `Scan -> Router -> Filter -> MemMove -> Project` forms one logical operator pipeline but three compiled pipelines:
        - First pipeline: `Scan` + push side of `Router`
        - Second pipeline: Pull side of `Router` to push side of `MemMove`
        - Third pipeline: Pull side of `MemMove` to `Project`
    - These breakers exist for implementation reasons (data movement, parallelization) but maintain logical flow

3. **Pipeline System Components**
    - **PipelineGen**: Abstract base class that generates LLVM IR for execution pipelines
    - **OlapCpuPipelineGen** and **OlapGpuPipelineGen**: Specialized generators for different hardware
    - **Pipeline**: Execution object created by PipelineGen to execute the compiled code
    - **PipelineGenFactory**: Factory for creating appropriate PipelineGen instances

### Execution Model

The execution model determines how pipelines are executed during query processing:

1. **Execution Flow**
    - `PreparedStatement::execute()` orchestrates pipeline execution
    - For the leaf compiled pipeline for each operator pipeline, it calls:
        - `Pipeline::open(session)`: Initializes state and allocates resources
        - `Pipeline::consume()`: Executes the compiled code
        - `Pipeline::close()`: Releases resources and performs cleanup

2. **Pipeline Connection Mechanism**
    - At pipeline boundaries, operators like `Router` connect compiled pipelines within an operator pipeline:
        - The \`Router::fire()\` method is executed during the *execution phase* by a worker thread responsible for the router's output partition.
        - The consumer pipeline is opened to initialize its state
        - Data is retrieved from a queue in a loop
        - For each unit of data, `pip->consume()` is called on the consumer pipeline
        - This continues until no more data is available
        - This mechanism enables data flow between compiled pipelines while preserving parallelism

3. **Pipeline Boundaries**
    - At compiled pipeline boundaries (where pipeline breakers occur), data is materialized in memory
    - The "push" side of an operator (e.g., Router) produces data into memory. Often this is just a pointer to a block of data if the data is packed.
    - The "pull" side of an operator consumes data from memory
    - This asynchronous connection enables parallel execution
    - A producer compiled pipeline can continue generating data while consumer compiled pipelines process earlier data

4. **Pipeline Parallelism**
    - Multiple compiled pipelines can be executed in parallel across threads and devices
    - Each pipeline may have a different degree of parallelism (DOP)
    - Routers and memory movement operators manage the data flow between parallel instances
    - This enables efficient utilization of heterogeneous hardware resources

### Produce/Consume Pattern: Compilation vs. Execution

It's critical to distinguish between two different phases where "produce" and "consume" terminology appears:

1. **Compilation Phase** (Operator::produce and Operator::consume)
    - **Operator::produce_**: Generates code for producing data
        - Called recursively top-down through the operator tree
        - Sets up the code generation context
        - Invokes child operators' produce methods
    - **Operator::consume**: Generates code for consuming data
        - Called recursively bottom-up through the operator tree
        - Defines how data will be processed in the generated code
        - Each operator generates its data processing logic here
    - These methods generate LLVM IR code during compilation, not data processing

2. **Execution Phase** (Pipeline::open, Pipeline::consume, Pipeline::close)
    - **Pipeline::open**: Initializes state for a compiled pipeline
    - **Pipeline::consume**: Executes the compiled code with actual data
        - This is where the physical data processing happens
        - Executes the code that was generated during compilation
    - **Pipeline::close**: Cleans up resources used by the pipeline
    - PreparedStatement::execute manages execution across all pipelines

3. **Connecting Compilation and Execution**
    - Operator::produce/consume methods generate code during compilation
    - The generated code is what executes when Pipeline::consume is called during execution
    - Pipeline breakers mark boundaries where data is materialized between compiled pipelines
    - Data flow during execution follows the paths established in the generated code

### Memory Movement and Device Crossing

Operators for moving data between devices and memory regions:

1. **Device Crossing Operators**
    - `to_gpu` (cpu2gpu): Copies data from CPU to GPU and transfers control by launching a GPU kernel
    - `to_cpu` (gpu2cpu): Transfers data from GPU to CPU and initiates CPU processing
        - Implemented in two parts (one on each device) communicating via an asynchronous queue
        - GPU part inserts tasks into the queue when data is ready to be sent to CPU
        - CPU part receives and processes the data
    - `memmove`: Moves data between NUMA nodes or GPUs
    - `membrdcst`: Broadcasts data to multiple execution units for parallel processing

2. **Hardware Heterogeneity Handling**
    - Device-crossing operators enable pipelined execution across heterogeneous hardware
    - They act as pipeline breakers that materialize data out of registers into memory
    - Other operators remain oblivious to hardware heterogeneity and execute on a single device
    - This approach eliminates the need for device-specific implementations of each operator

3. **Block Processing**
    - `pack`: Converts individual tuples into blocks for efficient processing and transfer
    - `unpack`: Converts blocks back into individual tuples when tuple-wise processing is needed
    - This enables efficient data movement and vectorized processing

### Routing and Parallelism

The routing system enables flexible data parallelism:

1. **Router Operator**
    - Distributes data across multiple processing units based on configurable policies
    - Acts as a compiled pipeline breaker for implementation reasons
    - Supports various routing policies:
        - Hash-based: Distributes data based on hash values
        - Random: Distributes data randomly for load balancing
        - Local: Distributes data randomly across data-local processing units
    - Handles parallel execution on a single node or across multiple nodes

2. **Split and Union**
    - `split`: Divides a data flow into multiple alternative processing paths
    - `unionAll`: Combines multiple data flows into a single flow
    - Together, these enable complex parallel execution strategies, such as:
        - Processing different parts of data on different hardware (CPU/GPU)
        - Dynamic load balancing across processing units
        - Implementing hybrid execution algorithms

3. **Affinitizers**
    - Control thread placement and memory locality
    - Map pipeline instances to specific CPU cores or NUMA nodes
    - Examples include:
        - `CpuNumaNodeAffinitizer`: Places threads on specific NUMA nodes
        - `GpuAffinitizer`: Handles GPU device assignment
    - Critical for performance optimization on multi-socket systems and heterogeneous hardware

### OlapParallelContext

The `OlapParallelContext` extends the codegen's `ParallelContext` to provide OLAP-specific functionality:

1. **OLAP-specific Code Generation**
    - Handles OLAP-specific IR code generation
    - Integrates with the codegen component for LLVM IR generation

2. **Pipeline Management**
    - Generates pipelines for different hardware targets
    - Manages pipeline state during query execution
    - Uses `OlapCpuPipelineGenFactory` and `OlapGpuPipelineGenFactory` to create pipelines

### Plugin System

The OLAP component uses a plugin system to handle different data formats and storage backends:

1. **Plugin Interface**
    - Abstract interface for data access plugins
    - Plugins implement methods for reading and writing data

2. **BinaryBlockPlugin**
    - Default high-performance plugin for analytics
    - Processes data in binary columnar format
    - Emits blocks of values that must be unpacked into tuples before processing by tuple-at-a-time operators

3. **Other Plugins**
    - Plugins for various data formats (JSON, CSV, etc.)
    - Storage-specific plugins (distributed storage, etc.)

### Relational Builder Interface

The `RelBuilder` class provides a high-level, fluent API for building query plans:

1. **Query Plan Construction**
    - Fluent interface (method chaining) for building operator trees
    - Example: `builder.scan(...).filter(...).project(...).print(...)`
    - Methods for creating all standard relational operations:
        - `scan`: Read data from a data source
        - `filter`: Apply predicates to filter rows
        - `project`: Select and transform columns
        - `join`: Combine data from multiple sources
        - `groupby`/`reduce`: Aggregate data
        - Routing and memory movement operations
    - Each method returns a new `RelBuilder` instance with the updated operator tree

2. **Expression System**
    - Uses the codegen's expression system for data manipulation
    - Lambda functions are heavily used to construct expressions in C++
    - Example:
      ```cpp
      .filter([&](const auto& arg) -> expression_t {
          return ge(arg["lo_suppkey"], 10);
      })
      ```

3. **Prepared Statements**
    - The `prepare()` method compiles the plan into a `PreparedStatement`
    - `PreparedStatement` objects can be executed repeatedly with different parameters
    - Execution is handled through the `execute()` method

## End-to-End Execution Flow

A complete overview of how queries are processed in the OLAP component:

1. **Plan Construction**
    - Query plans are built using the `RelBuilder` API
    - The operator tree is constructed with the desired operations
    - Parallelism settings, routing policies, and device assignments are specified
    - Example:
      ```cpp
      auto statement = builder
          .scan("lineorder.csv", {"lo_orderkey", "lo_quantity"}, catalog, pg{"block"})
          .router(DegreeOfParallelism{4}, 64, RoutingPolicy::HASH_BASED, DeviceType::CPU)
          .unpack()
          .filter([&](const auto& arg) { return gt(arg["lo_quantity"], 25); })
          .project([&](const auto& arg) { return std::vector{arg["lo_orderkey"]}; })
          .print(pg{"pm-csv"})
          .prepare();
      ```

2. **Plan Compilation**
    - The `prepare()` method initiates compilation into a `PreparedStatement`
    - During compilation:
        - `produce_()` method is called on the root operator (top-down traversal)
        - Operators generate LLVM IR code through their `produce_()` and `consume()` methods
        - The operator tree is segmented into pipelines (both operator and compiled) at pipeline breaker points
        - Code for each compiled pipeline is optimized and compiled to machine code
        - Pipeline objects are created to execute the compiled code

3. **Plan Execution**
    - `PreparedStatement::execute()` initiates the execution process
    - For each pipeline in the prepared statement:
        - `Pipeline::open(session)` initializes pipeline state and allocates resources
        - `Pipeline::consume()` executes the compiled code to process data
        - `Pipeline::close()` releases resources and cleans up
    - Results are collected by output operators (e.g., `print`) and returned to the user
    - Metrics about execution time and resource usage can be collected

## Relation to Codegen Component

The OLAP component builds on and extends the core/codegen component:

1. **Extended Classes**
    - `OlapParallelContext` extends `ParallelContext` with OLAP-specific functionality
    - `OlapCpuPipelineGenFactory` extends `CpuPipelineGenFactory` to create CPU pipeline generators
    - `OlapGpuPipelineGenFactory` extends `GpuPipelineGenFactory` to create GPU pipeline generators
    - These extensions add domain-specific features while leveraging the core functionality

2. **Integration Points**
    - OLAP operators generate code using the codegen expression system
        - Expressions for filtering, projection, etc. use the same expression hierarchy
        - The visitor pattern from codegen is used to generate LLVM IR for expressions
    - OLAP uses codegen pipelines for execution
        - Pipeline lifecycle (open, consume, close) is maintained
        - The same LLVM infrastructure is used for compilation
    - OLAP contexts handle state management and code generation like their codegen counterparts
