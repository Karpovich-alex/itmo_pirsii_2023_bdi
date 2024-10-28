package main

import (
	"fmt"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/index"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/measures"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
)

func main() {

	// Test Index
	dbIndex := index.FlatIndex{}
	v1 := utils.Vector{1, []float64{0, 0, 0, 0}}
	v2 := utils.Vector{2, []float64{0.1, 1, 1, 0.1}}
	v3 := utils.Vector{3, []float64{0, 0, 0, 1}}
	v4 := utils.Vector{4, []float64{0, 0, 0, 1}}
	vf := utils.Vector{0, []float64{0, 0, 0, 1}}

	dbIndex.AddVector(&v1)
	dbIndex.AddVector(&v2)
	dbIndex.AddVector(&v3)
	dbIndex.AddVector(&v4)

	dbIndex.RemoveVector(4)

	results := dbIndex.FindClosest(&vf, measures.EuclideanDistanceMeasure{}, 3)

	fmt.Println(results)

	err := dbIndex.Flush("./src/database/index.txt")
	if err != nil {
		panic(err)
	}
	newDbIndex := index.FlatIndex{}
	err = newDbIndex.Load("./src/database/index.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println(newDbIndex.FindById(1))
}
