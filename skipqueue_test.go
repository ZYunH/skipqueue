package skipqueue

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestSkipQueue_All(t *testing.T) {
	// Test random insert and delete.
	s := NewDefault()
	insertItemsArray := insertItems(10000)
	for i := 0; i < len(insertItemsArray); i++ {
		s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
	}
	for i := 0; i < len(insertItemsArray); i++ {
		v, ok := s.DeleteMin()
		if !ok || (v != strconv.Itoa(i)) {
			t.Fatalf("invalid numbers expected:%v ok:%v val:%v", i, ok, v)
		}
	}
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

func BenchmarkSkipQueue_Insert(b *testing.B) {
	insertItemsArray := insertItems(1000000)
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

func BenchmarkSkipQueue_DeleteMin(b *testing.B) {
	insertItemsArray := insertItems(1000000)
	s := NewDefault()
	for i := 0; i < len(insertItemsArray); i++ {
		s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.DeleteMin()
		}
	})
}

func BenchmarkSkipQueue_Insert_DeleteMin(b *testing.B) {
	insertItemsArray := insertItems(1000000)
	s := NewDefault()
	var i int
	for i < 1000 { // insert 1000 items before test
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
