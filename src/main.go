package main

import (
	"fmt"

	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/database"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
	
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/index"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/measures"

	// "github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/index"
	// "github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/measures"
	// "github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
)

func main() {

	dbIndex := index.FlatIndex{}

	v1 := utils.Vector{1, []float64{0, 0, 0, 0}}
	v2 := utils.Vector{2, []float64{0, 1, 1, 0}}
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


	foundVector, err := dbIndex.FindById(1)
	if err != nil {
		panic(err)
	}
	fmt.Println(foundVector)
	foundVector.Embedding[0] = 10
	foundVector.Embedding[1] = 20
	fmt.Println(foundVector)
	fmt.Println(dbIndex.FindById(1))

	err = dbIndex.Flush("./src/database/index.txt")

	if err != nil {
		panic(err)
	}
	newDbIndex := index.FlatIndex{}
	err = newDbIndex.Load("./database/index.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println(newDbIndex.FindById(1))

	dbs := new(database.DataBaseStruct)

	fmt.Println(dbs.Init("./source/db_info.txt"))

	db := new(database.DataBaseCollection)
	//fmt.Println(db.Init("test", "./source", dbs))
	// db.AddVector(v1)
	// db.AddVector(v2)
	// db.AddVector(v3)
	// db.AddVector(v4)
	// db.AddVector(vf)

	db.Load("test", dbs)

	//db.RemoveVector(2)

	//v0 := utils.Vector{1, []float64{5, 5, 5, 5}}
	//db.SetVector(2, v0)


	//fmt.Println(db.Flush("test", dbs))

	fmt.Println(db.FindById(3))

	//db2 := new(database.DataBaseCollection)
	//fmt.Println(db2.Init("test2", "./source", dbs))
	//fmt.Println(dbs.Remove("test2"))


}
