# Batch Processor

This Go package provides a flexible and configurable batch processing framework for concurrent execution of tasks in a controlled manner. It allows for efficient handling of batches of items using a supplier, processor, and finalizer.

## Usage

To use the batch processor, follow these steps:

1. **Create a Supplier:**
   - Implement the `Supplier` interface to fetch the next batch of items.

2. **Create a Processor:**
   - Implement the `Processor` interface to define how to process a batch of items.

3. **Create a Finalizer (Optional):**
   - Implement the `Finalizer` interface if you need to perform actions after each batch is processed.

4. **Instantiate the Batch Processor:**
    ```go
    processor := processor.NewBatchProcessorImpl(maxWorkersCount, yourSupplier, yourProcessor).
        WithFinalizer(yourFinalizer).
        WithNoBatchSleepIntervalInMilliseconds(sleepIntervalInMillis).
        WithProcessorTimeout(processorTimeoutInMillis)
    ```

5. **Start the Batch Processor:**
    ```go
    processor.Start()
    ```

6. **Stop the Batch Processor:**
    ```go
    processor.Stop()
    ```

## Interfaces

### `BatchProcessor[T any]`
- `Start()`: Initiates the batch processing loop.
- `Stop()`: Stops the batch processor, waiting for all processing to finish.

### `Supplier[T any]`
- `FetchNextBatch() ([]*T, error)`: Fetches the next batch of items for processing.

### `Processor[T any]`
- `ProcessBatch(ctx context.Context, batch []*T) ([]*T, error)`: Processes a batch of items with optional context and returns the processed items or an error.

### `Finalizer[T any]` (Optional)
- `OnBatchProcessed(processedBatch []*T, err error)`: Performs actions after process


## Mocking

Mocks were generated using https://vektra.github.io/mockery/latest/