package main

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockSFTPClient struct {
	files map[string][]byte
}

func (m *mockSFTPClient) Open(path string) (io.ReadCloser, error) {
	data, ok := m.files[path]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func BenchmarkTransferFiles(b *testing.B) {
	numFiles := 1000
	fileSize := 1024 * 500

	mockClient := &mockSFTPClient{
		files: make(map[string][]byte),
	}

	// Create test files
	jobs := make([]FileJob, numFiles)
	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("/remote/file_%d.bin", i)
		mockClient.files[path] = bytes.Repeat([]byte("x"), fileSize)
		jobs[i] = FileJob{
			RemotePath: path,
			ID:         fmt.Sprintf("id_%d", i),
		}
	}

	processFunc := func(result FileResult) error {
		if len(result.Data) == 0 {
			return fmt.Errorf("empty data for %s", result.ID)
		}
		return nil
	}

	cfg := DefaultCfg()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transferred, failed := cfg.TransferFilesMock(mockClient, jobs, processFunc)
		if failed > 0 {
			b.Fatalf("benchmark failed: %d files failed to transfer", failed)
		}
		if transferred != int32(numFiles) {
			b.Fatalf("expected %d transfers, got %d", numFiles, transferred)
		}
	}
}

func BenchmarkTransferFilesVariousConfigs(b *testing.B) {
	// Setup
	numFiles := 1000
	fileSize := 1024 * 100

	mockClient := &mockSFTPClient{
		files: make(map[string][]byte),
	}

	jobs := make([]FileJob, numFiles)
	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("/remote/file_%d.bin", i)
		mockClient.files[path] = bytes.Repeat([]byte("x"), fileSize)
		jobs[i] = FileJob{
			RemotePath: path,
			ID:         fmt.Sprintf("id_%d", i),
		}
	}

	processFunc := func(result FileResult) error {
		if len(result.Data) == 0 {
			return fmt.Errorf("empty data")
		}
		return nil
	}

	configs := []struct {
		name string
		cfg  PipelineCfg
	}{
		{"Default", DefaultCfg()},
		{"HighReaders", PipelineCfg{SFTPReaders: 200, Workers: 10, BufferSize: 10}},
		{"HighWorkers", PipelineCfg{SFTPReaders: 80, Workers: 50, BufferSize: 10}},
		{"LargeBuffer", PipelineCfg{SFTPReaders: 80, Workers: 10, BufferSize: 100}},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				transferred, failed := cfg.cfg.TransferFilesMock(mockClient, jobs, processFunc)
				if failed > 0 || transferred != int32(numFiles) {
					b.Fatalf("failed: transferred=%d, failed=%d", transferred, failed)
				}
			}
		})
	}
}

// TransferFilesMock is a test-friendly wrapper that accepts the mock client
func (cfg PipelineCfg) TransferFilesMock(client interface {
	Open(string) (io.ReadCloser, error)
}, jobs []FileJob, processFunc ProcessFunc,
) (transferred int32, failed int32) {
	jobsChan := make(chan FileJob, len(jobs))
	resultsChan := make(chan FileResult, cfg.BufferSize)
	start := time.Now()

	// Add Jobs to `jobsChan`
	go func() {
		for _, job := range jobs {
			jobsChan <- job
		}
		close(jobsChan)
	}()

	// Spin up Go Routine for each `job`
	var readWg sync.WaitGroup
	for i := 0; i < cfg.SFTPReaders; i++ {
		readWg.Go(func() {
			for job := range jobsChan {
				f, err := client.Open(job.RemotePath)
				if err != nil {
					atomic.AddInt32(&failed, 1)
					continue
				}
				data, err := io.ReadAll(f)
				f.Close()
				if err != nil {
					atomic.AddInt32(&failed, 1)
					continue
				}
				resultsChan <- FileResult{ID: job.ID, Data: data}
			}
		})
	}

	// Wait for Jobs to be Read
	go func() {
		readWg.Wait()
		close(resultsChan)
	}()

	// Sping up Go Routine to 'processFunc' foreach job
	var processWg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		processWg.Go(func() {
			for result := range resultsChan {
				if err := processFunc(result); err != nil {
					atomic.AddInt32(&failed, 1)
				} else {
					atomic.AddInt32(&transferred, 1)
				}
			}
		})
	}

	// Wait for `processFunc` to complete
	processWg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Transfer completed in %s. Success: %d, Failed: %d\n", elapsed, transferred, failed)
	return transferred, failed
}
