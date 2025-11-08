package datastructure

import (
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
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

	// Reverse the items
	utils.Reverse(items)

	// Add all items to the front of the queue first
	d.data = append(items, d.data...)

	// Record the new length after adding all items
	newLength := len(d.data)

	// Now satisfy waiters by removing items from the front
	for len(d.waiters) > 0 && len(d.data) > 0 {
		waiter := d.waiters[0]
		item := d.data[0]
		d.waiters = d.waiters[1:]
		d.data = d.data[1:]
		waiter.Ch <- item
	}

	// Return the length as if all items were added (before removing for waiters)
	return newLength
}

func (d *Deque[T]) PushBack(items ...T) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Add all items to the back of the queue first
	d.data = append(d.data, items...)

	// Record the new length after adding all items
	newLength := len(d.data)

	// Now satisfy waiters by removing items from the front
	for len(d.waiters) > 0 && len(d.data) > 0 {
		waiter := d.waiters[0]
		item := d.data[0]
		d.waiters = d.waiters[1:]
		d.data = d.data[1:]
		waiter.Ch <- item
	}

	// Return the length as if all items were added (before removing for waiters)
	return newLength
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

	itemsToRemove := min(len(d.data), n)
	item := d.data[:itemsToRemove]
	d.data = d.data[itemsToRemove:]
	return item, itemsToRemove
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

func (d *Deque[T]) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return fmt.Sprintf("Deque{%v}", d.data)
}
