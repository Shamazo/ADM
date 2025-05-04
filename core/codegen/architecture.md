# Core/Codegen Component Architecture

## Overview

The `core/codegen` component of Proteus is a Just-In-Time (JIT) code generation framework that enables dynamic generation, compilation, and execution of LLVM IR code. It's designed to efficiently translate high-level query expressions into executable code that can run on both CPU and GPU platforms, supporting heterogeneous hardware execution.

## Key Components

### Context Hierarchy

The context system provides an abstraction layer for code generation across different hardware targets:

1. **Context** (abstract base class)
    - Provides foundational code generation facilities
    - Manages LLVM IR builder and module
    - Handles basic block management and control flow generation
    - Utility functions for creating constants, types, and memory operations

2. **ParallelContext** (extends Context)
    - Higher-level abstraction for parallel execution environments
    - Manages multiple pipelines with state tracking
    - Supports both CPU and GPU execution contexts
    - Provides hardware-specific primitives (thread IDs, atomic operations, etc.)

### Pipeline System

The pipeline system handles the compilation and execution of generated code:

1. **PipelineGen** (abstract base class)
    - Generates LLVM IR for execution pipelines
    - Manages state variables and function arguments
    - Provides mechanisms for pipeline chaining and state sharing
    - Supports lifecycle methods (open, consume, close)

2. **CpuPipelineGen** (extends PipelineGen)
    - Specializes pipeline generation for CPU execution
    - Handles CPU-specific code generation and execution

3. **GpuPipelineGen** (extends PipelineGen)
    - Specializes pipeline generation for GPU execution
    - Manages device memory and kernel execution

4. **Pipeline** (execution object)
    - Created by PipelineGen to execute the compiled code
    - Maintains state during execution
    - Provides methods to open, consume data, and close the pipeline

5. **PipelineGenFactory**
    - Factory pattern for creating appropriate PipelineGen instances
    - Supports different hardware backends (CPU, GPU)

### Expression System

The expression system enables representation and evaluation of complex expressions:

1. **Expression** (abstract base class)
    - Base for all expression types
    - Provides visitor pattern support for traversal and evaluation

2. **ExprVisitor**
    - Visitor pattern implementation for processing expressions
    - Used to generate code from expressions
    - Supports different evaluation strategies (JIT, interpretation)

3. **Expressions**
    - Various expression types including:
        - Constants (Int, Float, String, etc.)
        - Binary operations (Add, Subtract, Multiply, etc.)
        - Comparisons (Equals, LessThan, etc.)
        - Logical operations (And, Or, etc.)
        - Conditional expressions (IfThenElse)
        - Record operations (Projection, Construction)
        - Special operations (Extract, Cast, etc.)

4. **expression_t**
    - Wrapper class for Expression instances
    - Provides operator overloading for intuitive expression construction
    - Automatic conversion from primitive types to expression objects

## Key Abstractions and Patterns

### State Management

The codegen component provides abstractions for managing state:

1. **StateVar**
    - Represents state variables that persist across pipeline invocations
    - Supports initialization and deinitialization

2. **Parameter Passing**
    - Allows passing parameters to pipelines during execution
    - Supports both static and dynamic parameter binding

### Control Flow Generation

Built-in mechanisms for generating control flow structures:

1. **Conditional Execution**
    - If-Then-Else constructs via `gen_if`
    - Support for nested conditions

2. **Loop Constructs**
    - While loops via `gen_while`
    - Do-While loops via `gen_do`

### Pipeline Chaining

Enables complex pipeline compositions:

1. **Sequential Execution**
    - Chain pipelines to execute one after another
    - Pass state between chained pipelines

2. **Nested Pipelines**
    - Support for hierarchical pipeline structures
    - Subpipeline invocation from parent pipelines

## Execution Flow

1. **Code Generation**
    - Create a Context/ParallelContext
    - Generate code using LLVM IR builder and expression visitors
    - Set up pipeline functions (open, consume, close)

2. **Compilation**
    - Compile generated LLVM IR code
    - Optimize code based on target architecture
    - Load compiled code into memory

3. **Execution**
    - Create Pipeline objects from compiled code
    - Open pipeline to initialize state
    - Consume data through the pipeline
    - Close pipeline to clean up resources

## Platform Support

1. **CPU Execution**
    - Uses LLVM JIT for native code generation
    - Optimized for multi-core CPU execution

2. **GPU Execution**
    - Generates CUDA code for NVIDIA GPUs
    - Manages device memory and kernel execution
    - Supports atomic operations and thread synchronization


## Usage Examples

Here's a simplified example of using the codegen component to create and execute a pipeline:

```cpp
// Create a parallel context (CPU-based)
auto context = ParallelContext::prepareParallelContext("test", false);

// Set up state variables if needed
auto stateVar = context->appendStateVar(
    llvm::PointerType::getUnqual(llvm::Type::getInt32Ty(context->getLLVMContext())),
    [=](llvm::Value* pip) -> llvm::Value* {
        auto mem = context->allocateStateVar(llvm::Type::getInt32Ty(context->getLLVMContext()));
        context->getBuilder()->CreateStore(context->createInt32(100), mem);
        return mem;
    },
    [=](llvm::Value* pip, llvm::Value* state_var) {
        context->deallocateStateVar(state_var);
    });

// Set up the main pipeline function
context->setGlobalFunction(true);

// Generate code using the expression system
auto expression = (expression_t{10} - expression_t{4}) / expression_t{3} +
                    expression_t{4} * expression_t{2};
auto result = value.accept(*visitor);
llvm::Function* printInt = context->getFunction("printi");
context->gen_call(printInt, {result.value});

// Compile and load the generated code
context->compileAndLoad();

// Get the compiled pipelines and execute
auto pipelines = context->getPipelines();
auto& pipeline = pipelines.front();

// Execute the pipeline
auto* session = MemoryManager::mallocPinned(sizeof(size_t));
pipeline->open(session);
pipeline->consume();
pipeline->close();
MemoryManager::freePinned(session);
```

## Extensibility

The codegen component is designed for extensibility:

1. **New Expression Types**
    - Can add new expression classes by extending Expression and updating visitors
    - Support for external function integration via ExternExpression

2. **New Hardware Targets**
    - Can support new hardware by implementing new PipelineGen subclasses
    - Abstraction layer allows hardware-specific optimization

3. **Custom Optimizations**
    - Can integrate custom LLVM passes for domain-specific optimization
    - Support for both high-level and low-level optimizations