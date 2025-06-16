package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/YanVdovin/TestTaskPipeline/lib"
)

// SourceNode traverses a directory and sends file paths to its output channel.
type SourceNode struct {
	lib.BaseNode
	dir string // Directory to scan
}

// NewSourceNode creates a new SourceNode for the given directory.
func NewSourceNode(dir string) *SourceNode {
	return &SourceNode{dir: dir}
}

// Process walks the directory and sends each file path to the output channel.
// It respects context cancellation and closes the output channel when done.
func (s *SourceNode) Process(ctx context.Context) {
	out := s.Outputs[0]
	_ = filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("error accessing path %q: %v", path, err)
			return nil
		}
		if !info.IsDir() {
			select {
			case out <- path:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	close(out)
}

// WorkerNode computes MD5 hashes for files received from its input channel.
type WorkerNode struct {
	lib.BaseNode
}

// NewWorkerNode creates a new WorkerNode for hash computation.
func NewWorkerNode() *WorkerNode {
	return &WorkerNode{}
}

// Process reads file paths from the input channel, computes their hashes,
// and sends results to the output channel. It closes the output when input is done.
func (w *WorkerNode) Process(ctx context.Context) {
	in := w.Inputs[0]
	out := w.Outputs[0]
	for {
		select {
		case val, ok := <-in:
			if !ok {
				close(out)
				return
			}
			path, ok := val.(string)
			if !ok {
				continue
			}
			hash, err := computeMD5(path)
			if err != nil {
				log.Printf("failed to hash %s: %v", path, err)
				continue
			}
			select {
			case out <- fmt.Sprintf("%s hash: %s", path, hash):
			case <-ctx.Done():
				close(out)
				return
			}
		case <-ctx.Done():
			close(out)
			return
		}
	}
}

// computeMD5 calculates the MD5 hash of a file at the given path.
// It returns the hex-encoded hash or an error if the file cannot be processed.
func computeMD5(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := md5.New()
	if _, err = io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// SinkNode collects and prints results from its input channels.
type SinkNode struct {
	lib.BaseNode
}

// NewSinkNode creates a new SinkNode for outputting results.
func NewSinkNode() *SinkNode {
	return &SinkNode{}
}

// Process reads from all input channels concurrently and logs the values.
// It uses a WaitGroup to ensure all inputs are processed before returning.
func (s *SinkNode) Process(ctx context.Context) {
	var wg sync.WaitGroup
	for _, in := range s.Inputs {
		wg.Add(1)
		go func(ch <-chan interface{}) {
			defer wg.Done()
			for {
				select {
				case val, ok := <-ch:
					if !ok {
						return
					}
					log.Println(val)
				case <-ctx.Done():
					return
				}
			}
		}(in)
	}
	wg.Wait()
}

// main sets up and runs the pipeline for the demonstration task.
func main() {
	dir := flag.String("dir", ".", "Directory to scan")
	workers := flag.Int("workers", 10, "Number of worker nodes")
	flag.Parse()

	ctx := context.Background()
	p := lib.NewPipeline()

	// Initialize source node to scan directory
	source := NewSourceNode(*dir)
	fileChan := make(chan interface{}, 100)
	if err := source.SetOutput(0, fileChan); err != nil {
		log.Fatalf("failed to set output for source: %v", err)
	}
	p.AddNode(source)

	// Create and connect worker nodes for parallel hash computation
	workerNodes := make([]*WorkerNode, *workers)
	workerOuts := make([]chan interface{}, *workers)
	for i := range *workers {
		worker := NewWorkerNode()
		out := make(chan interface{}, 10)
		if err := worker.SetInput(0, fileChan); err != nil {
			log.Fatalf("failed to set input for worker %d: %v", i, err)
		}
		if err := worker.SetOutput(0, out); err != nil {
			log.Fatalf("failed to set output for worker %d: %v", i, err)
		}
		workerNodes[i] = worker
		workerOuts[i] = out
		p.AddNode(worker)
	}

	// Initialize sink node to collect and print results
	sink := NewSinkNode()
	for i, out := range workerOuts {
		if err := sink.SetInput(i, out); err != nil {
			log.Fatalf("failed to set input %d for sink: %v", i, err)
		}
	}
	p.AddNode(sink)

	// Start the pipeline
	p.Start(ctx)
}
