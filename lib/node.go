// Package lib provides a framework for building concurrent data processing pipelines.
package lib

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Node represents a data processing unit with multiple inputs and outputs.
type Node interface {
	Process(ctx context.Context)
	SetInput(index int, ch <-chan interface{}) error
	SetOutput(index int, ch chan<- interface{}) error
}

// Pipeline manages and connects processing nodes.
type Pipeline struct {
	nodes []Node
}

// NewPipeline creates an empty pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{}
}

// AddNode adds a node to the pipeline.
func (p *Pipeline) AddNode(n Node) *Pipeline {
	p.nodes = append(p.nodes, n)
	return p
}

// Start runs all nodes concurrently and waits for them to finish.
func (p *Pipeline) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for _, node := range p.nodes {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			n.Process(ctx)
		}(node)
	}
	wg.Wait()
}

// Connect links the output of one node to the input of another.
func (p *Pipeline) Connect(from Node, fromOutput int, to Node, toInput int) error {
	ch := make(chan interface{})
	if err := from.SetOutput(fromOutput, ch); err != nil {
		return fmt.Errorf("connect: set output %d: %w", fromOutput, err)
	}
	if err := to.SetInput(toInput, ch); err != nil {
		return fmt.Errorf("connect: set input %d: %w", toInput, err)
	}
	return nil
}

// BaseNode provides reusable input/output logic for nodes.
type BaseNode struct {
	Inputs  []<-chan interface{}
	Outputs []chan<- interface{}
}

// SetInput assigns an input channel to a given index.
func (b *BaseNode) SetInput(index int, ch <-chan interface{}) error {
	if index < 0 {
		return errors.New("input index must be non-negative")
	}
	if index >= len(b.Inputs) {
		b.expandInputs(index + 1)
	}
	b.Inputs[index] = ch
	return nil
}

// SetOutput assigns an output channel to a given index.
func (b *BaseNode) SetOutput(index int, ch chan<- interface{}) error {
	if index < 0 {
		return errors.New("output index must be non-negative")
	}
	if index >= len(b.Outputs) {
		b.expandOutputs(index + 1)
	}
	b.Outputs[index] = ch
	return nil
}

func (b *BaseNode) expandInputs(n int) {
	newInputs := make([]<-chan interface{}, n)
	copy(newInputs, b.Inputs)
	b.Inputs = newInputs
}

func (b *BaseNode) expandOutputs(n int) {
	newOutputs := make([]chan<- interface{}, n)
	copy(newOutputs, b.Outputs)
	b.Outputs = newOutputs
}
