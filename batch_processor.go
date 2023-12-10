package processor

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type BatchProcessor[T any] interface {
	Start()
	Stop()
}

//go:generate mockery --name Supplier
type Supplier[T any] interface {
	FetchNextBatch() ([]*T, error)
}

//go:generate mockery --name Processor
type Processor[T any] interface {
	ProcessBatch(ctx context.Context, batch []*T) ([]*T, error)
}

//go:generate mockery --name Finalizer
type Finalizer[T any] interface {
	OnBatchProcessed(processedBatch []*T, err error)
}

const DefaultTimeout = time.Duration(2147483647) * time.Millisecond

type BatchProcessorImpl[T any] struct {
	maxWorkersCount      int32
	currentWorkersCount  int32
	supplier             Supplier[T]
	processor            Processor[T]
	finalizer            Finalizer[T]
	noBatchSleepInterval time.Duration
	processorTimeout     time.Duration
	isStopSignalReceived atomic.Bool
}

func NewBatchProcessorImpl[T any](maxWorkersCount int, supplier Supplier[T], processor Processor[T]) *BatchProcessorImpl[T] {
	if supplier == nil {
		panic(errors.New("BatchProcessorImpl::NewBatchProcessorImpl - supplier is missing"))
	}
	if processor == nil {
		panic(errors.New("BatchProcessorImpl::NewBatchProcessorImpl - processor is missing"))
	}

	return &BatchProcessorImpl[T]{
		maxWorkersCount:      int32(maxWorkersCount),
		currentWorkersCount:  0,
		supplier:             supplier,
		processor:            processor,
		noBatchSleepInterval: 1 * time.Second,
		processorTimeout:     DefaultTimeout,
		isStopSignalReceived: atomic.Bool{},
	}
}

// WithFinalizer /* Finalizers are useful if there is a need to run code after the batch is processed
func (s *BatchProcessorImpl[T]) WithFinalizer(finalizer Finalizer[T]) *BatchProcessorImpl[T] {
	s.finalizer = finalizer
	return s
}

// WithNoBatchSleepIntervalInMilliseconds /* For how long should the processor wait before trying to get the next batch of messages in case the previous fetch was empty
func (s *BatchProcessorImpl[T]) WithNoBatchSleepIntervalInMilliseconds(millis int64) *BatchProcessorImpl[T] {
	s.noBatchSleepInterval = time.Duration(millis) * time.Millisecond
	return s
}

func (s *BatchProcessorImpl[T]) WithProcessorTimeout(processorTimeoutMillis int64) *BatchProcessorImpl[T] {
	var processorTimeout time.Duration
	if processorTimeoutMillis <= 0 {
		processorTimeout = DefaultTimeout
	} else {
		processorTimeout = time.Duration(processorTimeoutMillis) * time.Millisecond
	}
	s.processorTimeout = processorTimeout
	return s
}

// Stop /* Stops the processor. It will wait until all the processing is finished.
func (s *BatchProcessorImpl[T]) Stop() {
	s.isStopSignalReceived.Store(true) // sends the shutdown signal to the batch processor
	// wait until all processing is stopped
	for {
		availableWorkersCount := s.getAvailableWorkersCount()
		if availableWorkersCount >= s.maxWorkersCount {
			// all workers have finished, we can stop the manager
			return
		}
		time.Sleep(10 * time.Millisecond) // not to overdo it when checking the atomic variable
	}
}

func (s *BatchProcessorImpl[T]) Start() {
	if s.isStopSignalReceived.Load() { // to make sure any Start() calls after Stop() will not start new routines needlessly
		return
	}

	go func() {
		for {
			if s.isStopSignalReceived.Load() {
				// this will stop the main processing loop so no new workers will be started
				return
			}
			s.tryProcessBatch()
		}
	}()
}

func (s *BatchProcessorImpl[T]) tryProcessBatch() {
	availableWorkersCount := s.getAvailableWorkersCount()
	if availableWorkersCount <= 0 {
		return
	}

	for i := 0; i < int(availableWorkersCount); i++ {
		if s.isStopSignalReceived.Load() {
			// stop any new potential workers from being started
			return
		}
		batch, err := s.supplier.FetchNextBatch()
		if err != nil {
			// TODO: introduce here retries with backoff strategy
			continue
		}
		if len(batch) <= 0 {
			if s.noBatchSleepInterval > 0 {
				time.Sleep(s.noBatchSleepInterval)
			}
			continue
		}
		if s.isStopSignalReceived.Load() {
			// supplier has provided the next batch, but we are in the shutdown mode, exit before processing
			return
		}
		s.processBatchAsync(batch)
		// NOTE: we have to sleep a bit here to give time for new processor routine to start, otherwise we might over provision the workers
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *BatchProcessorImpl[T]) processBatchAsync(batch []*T) {
	go func(batch []*T) {
		defer func() {
			s.updateWorkerCounter(-1)    // finished with batch of messages, release the worker
			s.recoverIfNeeded(recover()) // check if the worker panicked and handle
		}()

		s.updateWorkerCounter(1) // increase the counter of running workers

		// prepare the cancellable context for the processor in case of a timeout
		var cancelTimeout = s.processorTimeout
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(cancelTimeout, cancel)

		processed, err := s.processor.ProcessBatch(ctx, batch)
		if ctx.Err() != nil && err == nil {
			err = ctx.Err()
		}
		s.finalizeIfConfigured(processed, err)
	}(batch)
}

func (s *BatchProcessorImpl[T]) recoverIfNeeded(e any) {
	if e == nil {
		return
	}
	//NOTE: if we are here, it means that the worker panicked, so we need to finalize the batch
	err, ok := e.(error)
	if ok {
		s.finalizeIfConfigured(nil, err)
	} else {
		s.finalizeIfConfigured(nil, errors.New("panic in worker"))
	}
}

func (s *BatchProcessorImpl[T]) finalizeIfConfigured(batch []*T, err error) {
	if s.finalizer != nil {
		s.finalizer.OnBatchProcessed(batch, err)
	}
}

func (s *BatchProcessorImpl[T]) getAvailableWorkersCount() int32 {
	return s.maxWorkersCount - atomic.LoadInt32(&s.currentWorkersCount)
}

func (s *BatchProcessorImpl[T]) updateWorkerCounter(delta int32) {
	atomic.AddInt32(&s.currentWorkersCount, delta)
}
