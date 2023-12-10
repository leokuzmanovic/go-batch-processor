package processor

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type TestEvent struct {
}

func TestBatchProcessor(t *testing.T) {
	t.Run("the one with invalid supplier", func(t *testing.T) {
		defer func() {
			e := recover()
			err, ok := e.(error)
			assert.True(t, ok)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "supplier"))
		}()
		NewBatchProcessorImpl[TestEvent](1, nil, NewMockProcessor[TestEvent](t))
	})

	t.Run("the one with invalid processor", func(t *testing.T) {
		defer func() {
			e := recover()
			err, ok := e.(error)
			assert.True(t, ok)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "processor"))
		}()
		NewBatchProcessorImpl[TestEvent](1, NewMockSupplier[TestEvent](t), nil)
	})

	t.Run("slow batch processing so waiting for free workers", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}
		supplier.On("FetchNextBatch").Return(messages, nil)
		processor.On("ProcessBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			time.Sleep(150 * time.Millisecond)
		}).Return(nil, nil)

		batchProcessor := NewBatchProcessorImpl[TestEvent](1, supplier, processor)
		batchProcessor.tryProcessBatch()
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 1)
	})

	t.Run("batch processor timeout", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		finalizer := NewMockFinalizer[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}
		supplier.On("FetchNextBatch").Return(messages, nil)
		processor.On("ProcessBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			time.Sleep(50 * time.Millisecond)
		}).Return(messages, nil)
		finalizer.On("OnBatchProcessed", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {})

		batchProcessor := NewBatchProcessorImpl[TestEvent](1, supplier, processor).
			WithFinalizer(finalizer).
			WithProcessorTimeout(10)
		batchProcessor.tryProcessBatch()

		time.Sleep(100 * time.Millisecond) // have to sleep a bit since processor is running in a separate goroutine

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 1)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 1)
		finalizerErr, _ := finalizer.Calls[0].Arguments[1].(error)
		assert.Error(t, finalizerErr)
		assert.Equal(t, "context canceled", finalizerErr.Error())
	})

	t.Run("fast processor but not enough messages", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}
		supplier.On("FetchNextBatch").Return(messages, nil).Times(1)
		supplier.On("FetchNextBatch").Return(make([]*TestEvent, 0), nil)
		processor.On("ProcessBatch", mock.Anything, mock.Anything).Return(nil, nil)

		batchProcessor := NewBatchProcessorImpl[TestEvent](1, supplier, processor)
		// we would like to process messages on 3 occasions but only once we can provide some messages for the batch worker
		batchProcessor.tryProcessBatch()
		batchProcessor.tryProcessBatch()
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 3)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 1)
	})

	t.Run("error fetching messages from the supplier", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)

		supplier.On("FetchNextBatch").Return(nil, errors.New("some error")).Times(2)

		batchProcessor := NewBatchProcessorImpl[TestEvent](2, supplier, processor)
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 2)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 0)
	})

	t.Run("partial error fetching messages from supplier", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}
		supplier.On("FetchNextBatch").Return(nil, errors.New("some error")).Once()
		supplier.On("FetchNextBatch").Return(messages, nil)
		processor.On("ProcessBatch", mock.Anything, mock.Anything).Return(nil, nil)

		batchProcessor := NewBatchProcessorImpl[TestEvent](3, supplier, processor)
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 3)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 2)
	})

	t.Run("one error, one empty receive and one proper batch", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}
		supplier.On("FetchNextBatch").Return(nil, errors.New("some error")).Once()
		supplier.On("FetchNextBatch").Return(make([]*TestEvent, 0), nil).Once()
		supplier.On("FetchNextBatch").Return(messages, nil)
		processor.On("ProcessBatch", mock.Anything, mock.Anything).Return(nil, nil)

		batchProcessor := NewBatchProcessorImpl[TestEvent](3, supplier, processor).
			WithNoBatchSleepIntervalInMilliseconds(1)
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 3)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 1)
	})

	t.Run("processor errors do not affect the processor's attempts", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		finalizer := NewMockFinalizer[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}

		supplier.On("FetchNextBatch").Return(messages, nil)
		processorError := errors.New("some error")
		processor.On("ProcessBatch", mock.Anything, messages).Return(nil, processorError)
		finalizer.On("OnBatchProcessed", mock.Anything, processorError).Run(func(args mock.Arguments) {})

		batchProcessor := NewBatchProcessorImpl[TestEvent](3, supplier, processor).
			WithFinalizer(finalizer).
			WithNoBatchSleepIntervalInMilliseconds(1)
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 3)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 3)
		finalizer.AssertNumberOfCalls(t, "OnBatchProcessed", 3)
	})

	t.Run("processor panics", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		finalizer := NewMockFinalizer[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}

		supplier.On("FetchNextBatch").Return(messages, nil)
		processor.On("ProcessBatch", mock.Anything, messages).Run(func(args mock.Arguments) { panic("problem") })
		finalizer.On("OnBatchProcessed", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {})

		batchProcessor := NewBatchProcessorImpl[TestEvent](1, supplier, processor).
			WithFinalizer(finalizer).
			WithNoBatchSleepIntervalInMilliseconds(1)
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 1)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 1)
		finalizer.AssertNumberOfCalls(t, "OnBatchProcessed", 1)
		finalizerErr, ok := finalizer.Calls[0].Arguments[1].(error)
		assert.True(t, ok)
		assert.Equal(t, "panic in worker", finalizerErr.Error())
	})

	t.Run("processor panics with error", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		finalizer := NewMockFinalizer[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}

		supplier.On("FetchNextBatch").Return(messages, nil)
		processorError := errors.New("some error")
		processor.On("ProcessBatch", mock.Anything, messages).Run(func(args mock.Arguments) { panic(processorError) })
		finalizer.On("OnBatchProcessed", mock.Anything, processorError).Run(func(args mock.Arguments) {})

		batchProcessor := NewBatchProcessorImpl[TestEvent](1, supplier, processor).
			WithFinalizer(finalizer).
			WithNoBatchSleepIntervalInMilliseconds(1)
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 1)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 1)
		finalizer.AssertNumberOfCalls(t, "OnBatchProcessed", 1)
	})

	t.Run("the one with finalizer with successful processor", func(t *testing.T) {
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		finalizer := NewMockFinalizer[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}
		supplier.On("FetchNextBatch").Return(messages, nil)
		processor.On("ProcessBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {}).Return(messages, nil)
		finalizer.On("OnBatchProcessed", messages, nil).Run(func(args mock.Arguments) {})

		batchProcessor := NewBatchProcessorImpl[TestEvent](1, supplier, processor).
			WithFinalizer(finalizer).
			WithNoBatchSleepIntervalInMilliseconds(1)
		batchProcessor.tryProcessBatch()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 1)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 1)
		finalizer.AssertNumberOfCalls(t, "OnBatchProcessed", 1)
	})

	t.Run("stop is called on a running processor", func(t *testing.T) {
		/*
			Here we want to start the processor with 2 workers and call shutdown after the first worker is started.
			We expect that the second worker will not start and that the processor will wait for the first supplier
			to finish before returning from Stop().
		*/
		supplier := NewMockSupplier[TestEvent](t)
		processor := NewMockProcessor[TestEvent](t)
		finalizer := NewMockFinalizer[TestEvent](t)
		messages := make([]*TestEvent, 1)
		messages[0] = &TestEvent{}

		// make supliers and processor take some time to finish
		supplier.On("FetchNextBatch").Run(func(args mock.Arguments) {
			time.Sleep(50 * time.Millisecond)
		}).Return(messages, nil)
		processor.On("ProcessBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			time.Sleep(150 * time.Millisecond)
		}).Return(nil, nil)
		finalizer.On("OnBatchProcessed", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {})
		finalizer.On("OnBatchProcessed", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {})

		batchProcessor := NewBatchProcessorImpl[TestEvent](2, supplier, processor).
			WithFinalizer(finalizer).
			WithNoBatchSleepIntervalInMilliseconds(1)
		batchProcessor.Start()
		time.Sleep(60 * time.Millisecond) // have some delay when calling shutdown so the second worker will find the processor in shutdown state
		batchProcessor.Stop()

		supplier.AssertNumberOfCalls(t, "FetchNextBatch", 1)
		processor.AssertNumberOfCalls(t, "ProcessBatch", 1)
		finalizer.AssertNumberOfCalls(t, "OnBatchProcessed", 1)
	})
}
