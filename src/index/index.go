package index

import (
	"container/heap"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/measures"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
)

type SearchResult struct {
	Vector   utils.Vector
	Distance float64
}

type IndexStruct struct {
	ID       int
	VectorId int
	Result   []float64
}

func remove(s []*utils.Vector, i int) []*utils.Vector {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

type Index interface {
	create(v utils.Vector) (index IndexStruct, err error)
	update(id int) (index IndexStruct, err error)

	AddVector(v utils.Vector) (err error)
	RemoveVector(id int) (err error)

	Load() (err error)
	Flush() (err error)

	FindClosest(index IndexStruct, measure measures.Measure, n int) (results []SearchResult, err error)
}

type FlatIndex struct {
	vectors []*utils.Vector
}

func (index *FlatIndex) AddVector(v utils.Vector) {
	index.vectors = append(index.vectors, &v)
}

func (index *FlatIndex) RemoveVector(id int) {

	for idx, v := range index.vectors {
		if v.ID == id {
			index.vectors = remove(index.vectors, idx)
			return
		}
	}
	return
}

func (index *FlatIndex) FindClosest(v utils.Vector, measure measures.Measure, n int) (results []SearchResult) {
	pq := priorityQueue{}

	for _, va := range index.vectors {
		dist := measure.Calc(v, *va)
		heap.Push(&pq, &queueItem{
			value:    *va,
			priority: -dist,
		})
		if pq.Len() > n {
			heap.Pop(&pq)
		}
	}
	for i := pq.Len() - 1; i >= 0; i-- {
		qi := pq[i]
		results = append(results, SearchResult{qi.value, -qi.priority})
	}
	return results
}
