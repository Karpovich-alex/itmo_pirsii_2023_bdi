package main

import (
	"errors"
	"fmt"
	"os"
)

// интерфейс коллекции
type DataBase interface {
	init(name string, path string, databasestruct DataBaseStruct) (err error)

	// load() (err error)
	// flush() (err error)

	// addVector(id int, v Vector) (err error)
	// setVector(id int, v Vector) (err error)
	// removeVector(id int) (err error)

	// findById(id int) (v Vector, err error)
	// findClosest(v Vector, measure Measure, n int) (results []SearchResult, err error)
}

// одна коллекция в БД
type DataBaseCollection struct {
	ID   int
	Name string
	Path string
	// Vector []Vector
	// Index  []IndexStruct
}

// собственно сама БД
type DataBaseStruct struct {
	ID_count int
	Ref      map[string]DataBaseCollection //словарь - имя коллекции: ссылка на нее
}

// интерфейс БД
type DataBaseStructInterface interface {
	remove(name string) (err error)
}

func (db DataBaseCollection) init(name string, path string, dbs DataBaseStruct) error {
	info_path, err_path := os.Stat(path)
	if os.IsNotExist(err_path) {
		return errors.New("Path doesn't exist!")
	} else {
		if !info_path.IsDir() {
			return errors.New("Path should point to the directory!")
		} else {
			full_path := path + "/" + name + ".txt"

			_, err_file := os.Stat(full_path)
			if !os.IsNotExist(err_file) {
				return errors.New("Database with that name already exists!")
			} else {
				file, err := os.Create(full_path)
				if err != nil {
					return err
				}
				defer file.Close()

				db.Name = name
				db.Path = path
				db.ID = dbs.ID_count + 1

				// db_pointer := reflect.ValueOf(&db)

				// db_pointer.FieldByName("Name").SetString(name)
				// db_pointer.FieldByName("Path").SetString(full_path)
				// db_pointer.FieldByName("ID").SetInt(int64(dbs.ID_count) + 1)
				// db_pointer.FieldByName("Name").SetString(name)

				// dbs_pointer := reflect.ValueOf(&dbs)
				// dbs_pointer.FieldByName("ID").SetInt(int64(dbs.ID_count) + 1)

				dbs.ID_count = dbs.ID_count + 1
				dbs.Ref[name] = db

			}
		}
	}
	return nil

}

// удаляем коллекцию из БД
func (dbs DataBaseStruct) remove(name string) error {
	db := dbs.Ref[name]
	e := os.Remove(db.Path)
	return e
}

func main() {

	dbs := DataBaseStruct{ID_count: 0, Ref: make(map[string]DataBaseCollection)}

	db := DataBaseCollection{}
	fmt.Println(db.init("test", "./database", dbs))

}
