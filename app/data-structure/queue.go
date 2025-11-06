package datastructure

import (
	"sync"
	"time"
)

type Deque[T any] struct {
	mu      sync.Mutex
	data    []T
	waiters []*Waiter[T]
}

type Waiter[T any] struct {
	Ch chan T
}

func NewDeque[T any]() *Deque[T] {
	return &Deque[T]{data: make([]T, 0), waiters: make([]*Waiter[T], 0)}
}

func (d *Deque[T]) Kind() RType {
	return RList
}

func AsList[T any](v RValue) (*Deque[T], bool) {
	if lv, ok := v.(*Deque[T]); ok {
		return lv, true
	}
	return nil, false
}

func (d *Deque[T]) PushFront(items ...T) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If there are waiters, satisfy them first
	for len(d.waiters) > 0 && len(items) > 0 {
		waiter := d.waiters[0]
		item := items[0]
		d.waiters = d.waiters[1:]
		items = items[1:]
		waiter.Ch <- item
	}

	// Add remaining items to the front of the queue
	if len(items) > 0 {
		d.data = append(items, d.data...)
	}

	return len(d.data)
}

func (d *Deque[T]) PushBack(items ...T) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If there are waiters, satisfy them first
	for len(d.waiters) > 0 && len(items) > 0 {
		waiter := d.waiters[0]
		item := items[0]
		d.waiters = d.waiters[1:]
		items = items[1:]
		waiter.Ch <- item
	}

	// Add remaining items to the back of the queue
	if len(items) > 0 {
		d.data = append(d.data, items...)
	}

	return len(d.data)
}

func (d *Deque[T]) PopFront(timeout float64) (T, bool) {
	var zero T

	d.mu.Lock()
	// If there is at least one item in the queue, pop it
	if len(d.data) > 0 {
		item := d.data[0]
		d.data = d.data[1:]
		d.mu.Unlock()
		return item, true
	}

	// Queue is empty, register as waiter
	w := &Waiter[T]{Ch: make(chan T, 1)}
	d.waiters = append(d.waiters, w)
	d.mu.Unlock()

	// Indefinite wait
	if timeout <= 0 {
		item := <-w.Ch
		// Producer must have removed the waiter
		return item, true
	}

	// Definite timeout
	timer := time.NewTimer(time.Duration(timeout * float64(time.Second)))
	defer timer.Stop()

	select {
	case item := <-w.Ch:
		// Producer must have removed the waiter
		return item, true
	case <-timer.C:
		// Remove the waiter from the list if still present
		d.mu.Lock()
		for i, waiter := range d.waiters {
			if waiter == w {
				d.waiters = append(d.waiters[:i], d.waiters[i+1:]...)
				d.mu.Unlock()
				return zero, false
			}
		}
		d.mu.Unlock()
		// Waiter was already removed by producer, wait for item
		item := <-w.Ch
		return item, true
	}
}

func (d *Deque[T]) TryPop() (T, bool) {
	var zero T

	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.data) == 0 {
		return zero, false
	}

	item := d.data[0]
	d.data = d.data[1:]
	return item, true
}

func (d *Deque[T]) TryPopN(n int) ([]T, int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.data) == 0 {
		return nil, 0
	}

	items_to_remove := min(len(d.data), n)
	item := d.data[:items_to_remove]
	d.data = d.data[items_to_remove:]
	return item, items_to_remove
}

func (d *Deque[T]) Length() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.data)
}

func (d *Deque[T]) LRange(left, right int) []T {
	d.mu.Lock()
	defer d.mu.Unlock()

	n := len(d.data)
	if n == 0 {
		return make([]T, 0)
	}

	// Handle negative indices
	if right < 0 {
		right += n
	}
	if left < 0 {
		left += n
	}

	// Clamp to valid range
	right = max(0, min(right, n-1))
	left = max(0, min(left, n-1))

	if left > right {
		return make([]T, 0)
	}

	return d.data[left : right+1]
}
