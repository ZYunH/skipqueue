package skipqueue

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ZYunH/lockedsource"
)

const (
	defaultMaxLevel = 32
	defaultP        = 1 / math.E
	defaultRandSeed = 0
)

type SkipQueue struct {
	header *Node
	level  int
	length int64

	maxLevel int
	p        float64
	rnd      *rand.Rand
}

func NewDefault() *SkipQueue {
	return New(defaultMaxLevel, defaultP, defaultRandSeed)
}

func New(maxlevel int, p float64, randseed int64) *SkipQueue {
	if maxlevel <= 1 || p <= 0 {
		panic("maxLevel must greater than 1, p must greater than 0")
	}

	s := &SkipQueue{
		level:    1,
		maxLevel: maxlevel,
		p:        p,
		rnd:      rand.New(lockedsource.New(randseed)),
	}

	s.header = newNode(s.maxLevel, 0, "")
	return s
}

func (s *SkipQueue) randomLevel() int {
	level := 1

	for s.rnd.Float64() < s.p {
		level++
	}

	if level > s.maxLevel {
		return s.maxLevel
	}
	return level
}

func (s *SkipQueue) Insert(score int64, val string) *Node {
	update := make([]*Node, s.maxLevel)

	// Search the insert location, also calculates `update`.
	// The search process is begin from the highest level's header.
	n := s.header
	for i := s.maxLevel - 1; i >= 0; i-- {
		for n.levels[i].next != nil && n.levels[i].next.score < score {
			n = n.levels[i].next
		}
		update[i] = n
	}

	n = getLock(n, score, 0)
	if n.levels[0].next != nil && n.levels[0].next.score == score {
		n.levels[0].next.val = val
		n.levels[0].mu.Unlock()
		return n.levels[0].next
	}

	// Make a random level for the insert node.
	level := s.randomLevel()
	// If the insert process will create new levels, we need to
	// update the `update`.
	if level > s.level {
		for i := s.level; i < level; i++ {
			// s.header is the only node in every levels.
			update[i] = s.header
		}
		s.level = level
	}

	// Insert the new node.
	nn := newNode(level, score, val)

	// Lock entire new node.
	for i := 0; i < level; i++ {
		nn.levels[i].mu.Lock()
	}

	for i := 0; i < level; i++ {
		if i != 0 {
			n = getLock(update[i], score, i)
		}
		nn.levels[i].next = n.levels[i].next
		n.levels[i].next = nn
		n.levels[i].mu.Unlock()
	}

	// Unlock entire new node.
	for i := 0; i < level; i++ {
		nn.levels[i].mu.Unlock()
	}

	atomic.AddInt64(&s.length, 1)
	nn.timestamp = time.Now().UnixNano()
	return n
}

// key == score, val == val, comparisons are done on the key, the value is just the stored item.
func (s *SkipQueue) DeleteMin() (string, bool) {
	now := time.Now().UnixNano()

	// Try to find an existing node.
	n := s.header.levels[0].next
	for n != nil {
		if n.timestamp < now {
			if atomic.CompareAndSwapUint32(&n.deleted, 0, 1) {
				break
			}
		}
		n = n.levels[0].next
	}

	if n == nil { // can not find an existing node for now
		return "", false
	}
	score, val := n.score, n.val

	// Search update path for this node.
	update := make([]*Node, s.maxLevel)
	n = s.header
	for i := s.maxLevel - 1; i >= 0; i-- {
		for n.levels[i].next != nil && n.levels[i].next.score < score {
			n = n.levels[i].next
		}
		update[i] = n
	}

	// Make sure we have a pointer to the node(the score could be repeat).
	n = n.levels[0].next
	for n.val != val {
		n = n.levels[0].next
	}

	// Lock entire node.
	for i := 0; i < len(n.levels); i++ {
		n.levels[i].mu.Lock()
	}

	// n is the node to be deleted.
	for i := len(n.levels) - 1; i >= 0; i-- {
		pn := getLock(update[i], score, i) // lock the previous node
		pn.levels[i].next = n.levels[i].next
		n.levels[i].next = pn
		n.levels[i].mu.Unlock()  // unlock the deleted node
		pn.levels[i].mu.Unlock() // and the previous node
	}
	return val, true
}

func (s *SkipQueue) print() {
	for i := s.level - 1; i >= 0; i-- {
		print(i, " ")
		x := s.header.levels[i].next

		for x != nil {
			print("[val:", x.val, " score:", x.score, " deleted:", x.deleted, "] -> ")
			x = x.levels[i].next
		}
		print("nil")
		print("\r\n")
	}
	print("\r\n")
}

type Node struct {
	val       string
	score     int64
	deleted   uint32 // 0 is deleted, others not.
	timestamp int64
	levels    []_nodeLevel
}

type _nodeLevel struct {
	next *Node
	mu   sync.Mutex
}

func newNode(level int, score int64, val string) *Node {
	return &Node{
		val:       val,
		score:     score,
		deleted:   0,
		timestamp: math.MaxInt64,
		levels:    make([]_nodeLevel, level),
	}
}

func getLock(node *Node, score int64, level int) *Node {
	x := node.levels[level].next
	for x != nil && x.score < score {
		node = x
		x = node.levels[level].next
	}
	node.levels[level].mu.Lock()
	x = node.levels[level].next
	for x != nil && x.score < score {
		node.levels[level].mu.Unlock()
		node = x
		node.levels[level].mu.Lock()
		x = node.levels[level].next
	}
	return node
}
