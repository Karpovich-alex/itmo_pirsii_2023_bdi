package main

import (
	"fmt"

	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/database"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
	
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/index"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/measures"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/database"
)

func main() {

	dbIndex := index.FlatIndex{}
	// v1 := utils.Vector{1, []float64{0, 0, 0, 0}}
	// v2 := utils.Vector{2, []float64{0, 1, 1, 0}}
	// v3 := utils.Vector{3, []float64{0, 0, 0, 1}}
	// v4 := utils.Vector{4, []float64{0, 0, 0, 1}}
	vf := utils.Vector{0, []float64{0, 0, 0, 1}}

	//Создаем или читаем БД
	dbs := new(database.DataBaseStruct)
	dbs.Init("./source/db_info.txt")

	//Инициализируем коллекцию (даже если она есть)
	db := new(database.DataBaseCollection)
	db.Init("test", "./source", dbs)

	// Если коллеция уже есть
	db.Load("test", dbs)

	// Добавляем в коллекцию вектора
	// db.AddVector(v1)
	// db.AddVector(v2)
	// db.AddVector(v3)
	// db.AddVector(v4)

	// Сохраняем на диск
	// db.Flush("test", dbs)


	// Поиск ближайщих
	results := dbIndex.FindClosest(&vf, measures.EuclideanDistanceMeasure{}, 3)
	fmt.Println(results)


	//Поиск по ID
	foundVector, err := db.FindById(1)
	if err != nil {
		panic(err)
	}
	fmt.Println(foundVector)

	//
	foundVector.Embedding[0] = 10
	foundVector.Embedding[1] = 20
	fmt.Println(foundVector)
	fmt.Println(db.FindById(1))

	//Удаление вектора
	db.RemoveVector(2)
	fmt.Println(db)

	// Обновление вектора
	v0 := utils.Vector{1, []float64{5, 5, 5, 5}}
	db.SetVector(2, v0)
	fmt.Println(db)

	//Создание новой коллекции
	db2 := new(database.DataBaseCollection)
	fmt.Println(db2.Init("test2", "./source", dbs))

	// Удаление коллекции
	fmt.Println(dbs.Remove("test2"))

}
