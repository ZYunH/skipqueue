package skipqueue

import (
	"math/rand"
	"testing"
)

var insertItemsArray = insertItems()

type insertItem struct {
	score int64
	val   string
}

func insertItems() []insertItem {
	length := 30000000
	res := make([]insertItem, length)
	for i := 0; i < length; i++ {
		res[i] = insertItem{
			score: int64(rand.Intn(100000)),
			val:   "*****************************",
		}
	}
	return res
}

func BenchmarkNewSkipQueue(b *testing.B) {
	var i int
	s := New(32, 0.25, 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Insert(insertItemsArray[i].score, insertItemsArray[i].val)
			i++
		}
	})
}
