package main

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/sftp"
)

type FileJob struct {
	RemotePath string
	ID         string
}
type FileResult struct {
	ID   string
	Data []byte
}

type ProcessFunc func(result FileResult) error

type PipelineCfg struct {
	SFTPReaders int
	Workers     int
	BufferSize  int
}

func DefaultCfg() PipelineCfg {
	return PipelineCfg{
		SFTPReaders: 80,
		Workers:     10,
		BufferSize:  10,
	}

}

func (cfg PipelineCfg) TransferFiles(sftpClient *sftp.Client, jobs []FileJob, processFunc ProcessFunc) (transferred int32, failed int32) {

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
				f, err := sftpClient.Open(job.RemotePath)
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

	fmt.Printf("Transfer completed in %s. Success: %d, Failed: %d\n", time.Since(start), transferred, failed)

	return transferred, failed
}
