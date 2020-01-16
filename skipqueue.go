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
	defaultP        = 0.25
	defaultRandSeed = 0
)

// SkipQueue represent an concurrent-safe priority queue base on skiplist.
type SkipQueue struct {
	header *Node
	level  int
	length int64

	maxLevel int
	p        float64
	rnd      *rand.Rand
}

// NewDefault return a skipqueue with default configs.
func NewDefault() *SkipQueue {
	return New(defaultMaxLevel, defaultP, defaultRandSeed)
}

// New return a skipqueue with input configs.
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

// Insert a node into skipqueue, the score must be unique.
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

	// Lock the previous node, the returnd n represent the previous node.
	// We lock the node in level 0, so it can't be deleted by other goroutines.
	n = getLock(n, score, 0)

	// The previous node's next node has same score with the node to be inserted,
	// just change it's val, since the score is unique.
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
	nn.mu.Lock()

	for i := 0; i < level; i++ {
		if i != 0 { // Don't lock previous in level 0, since it has been locked.
			n = getLock(update[i], score, i)
		}
		// Insert new node.
		nn.levels[i].next = n.levels[i].next
		n.levels[i].next = nn
		// Unlock the previous node.
		n.levels[i].mu.Unlock()
	}

	// Unlock entire new node.
	nn.mu.Unlock()

	atomic.AddInt64(&s.length, 1)
	nn.timestamp = time.Now().UnixNano()
	return nn
}

// DeleteMin deletes a node with smallest score in skipqueue.
// Comparisons are done on the key, the value is just the stored item.
func (s *SkipQueue) DeleteMin() (string, bool) {
	now := time.Now().UnixNano()

	// Try to find an existing node.
	n := s.header.levels[0].next
	for n != nil {
		// Ignore all nodes that were inserted after the search process began.
		if n.timestamp < now {
			if atomic.CompareAndSwapUint32(&n.deleted, 0, 1) {
				break
			}
		}
		n = n.levels[0].next
	}

	if n == nil { // can't find an existing node for now
		return "", false
	}

	// Save the its score and val.
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

	// Make sure we have a pointer to the real node.
	// Notice that the score is unique.
	n = n.levels[0].next
	for n.score != score {
		n = n.levels[0].next
	}

	// Lock entire node.
	n.mu.Lock()

	// n is the node to be deleted.
	for i := len(n.levels) - 1; i >= 0; i-- {
		pn := getLock(update[i], score, i) // lock the previous node
		n.levels[i].mu.Lock()              // and deleted node in level i
		pn.levels[i].next = n.levels[i].next
		n.levels[i].next = pn
		n.levels[i].mu.Unlock()  // unlock the deleted node
		pn.levels[i].mu.Unlock() // and the previous node in level i
	}

	// Unlock entire node.
	n.mu.Unlock()

	return val, true
}

// for debug only.
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

// Node represents a skipqueue node.
type Node struct {
	val       string
	score     int64
	deleted   uint32 // 0 represents the node is exists
	timestamp int64
	mu        sync.Mutex
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

// getLock acquire a lock on the node which has the largest score
// which is smaller than the input score, the returned node in
// level i is locked.
func getLock(node *Node, score int64, level int) *Node {
	// Find the node with largest key which is smaller than the input score.
	x := node.levels[level].next
	for x != nil && x.score < score {
		node = x
		x = node.levels[level].next
	}
	node.levels[level].mu.Lock() // lock this node
	x = node.levels[level].next  // check if some nodes inserted in this process.
	// Something changed, x represents the next node.
	for x != nil && x.score < score {
		node.levels[level].mu.Unlock() // unlock previous node
		node = x
		node.levels[level].mu.Lock() // lock next node to prevent another insert
		x = node.levels[level].next  // go on the next node
	}
	// Now the node which has the largest score which is smaller
	// than the input score has been locked.
	return node
}
