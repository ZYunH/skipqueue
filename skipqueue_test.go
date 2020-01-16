package skipqueue

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

func checkNode(t *testing.T, score int64, val string, n *Node) {
	if score != n.score || val != n.val {
		t.Fatalf("expected %v `%v`, got %v `%v`", score, val, n.score, n.val)
	}
}

func TestSkipQueue_All(t *testing.T) {
	// Test simple insert.
	s := NewDefault()
	node1 := s.Insert(1, "1")
	checkNode(t, 1, "1", node1)

	node2 := s.Insert(2, "2")
	checkNode(t, 2, "2", node2)
	if node1.levels[0].next != node2 {
		t.Fatalf("invalid priority")
	}

	// Test random insert and delete.
	s = NewDefault()
	insertItemsArray := insertItems(100000)
	for i := 0; i < len(insertItemsArray); i++ {
		s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
	}
	for i := 0; i < len(insertItemsArray); i++ {
		v, ok := s.DeleteMin()
		if !ok || (v != strconv.Itoa(i)) {
			t.Fatalf("invalid numbers expected:%v ok:%v val:%v", i, ok, v)
		}
	}

	// Test concurrent.
	s = NewDefault()
	var wg sync.WaitGroup
	wg.Add(len(insertItemsArray))
	for i := 0; i < len(insertItemsArray); i++ {
		if rand.Intn(3) == 0 {
			go func() {
				s.DeleteMin()
				wg.Done()
			}()
		} else {
			go func(i int) {
				s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
}

type insertItem struct {
	score int64
	val   string
	rnd   int
}

func insertItems(length int) []insertItem {
	res := make([]insertItem, length)

	// Make unique random scores.
	scores := make(map[int64]struct{}, length)
	for i := 0; i < length; i++ {
		scores[int64(i)] = struct{}{}
	}

	var i int
	for k := range scores {
		res[i] = insertItem{
			score: k,
			val:   strconv.Itoa(int(k)),
			rnd:   rand.Intn(3),
		}
		i++
	}
	return res
}

// Benchmarks.
var benchArray = &benchArrayCache{}

type benchArrayCache struct {
	data []insertItem
	mu   sync.Mutex
}

func (c *benchArrayCache) get() *[]insertItem {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = insertItems(10000000)
	}
	return &c.data
}

func BenchmarkSkipQueue_Insert(b *testing.B) {
	insertItemsArray := *(benchArray.get())
	s := NewDefault()
	var i int
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
			i++
		}
	})
}

func BenchmarkSkipQueue_Insert_DeleteMin(b *testing.B) {
	insertItemsArray := *(benchArray.get())
	s := NewDefault()
	var i int
	for i < 2500 { // insert 2500 items before test
		s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
		i++
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if insertItemsArray[i].rnd == 0 {
				s.DeleteMin()
			} else {
				s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
			}
			i++
		}
	})
}
