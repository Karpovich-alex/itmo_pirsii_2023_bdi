package main

type Measure interface {
	calc(v1 Vector, v2 Vector) (dist float64, err error)
}

type Index interface {
	create(v Vector) (index IndexStruct, err error)
	update(id int) (index IndexStruct, err error)

	load() (err error)
	unload() (err error)

	removeVector(id int) (err error)
	findClosest(index IndexStruct, measure Measure, n int) (results []SearchResult, err error)
}
