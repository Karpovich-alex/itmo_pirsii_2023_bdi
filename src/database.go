package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
)

// интерфейс коллекции
type DataBase interface {
	init(name string, path string, databasestruct DataBaseStruct) (err error)

	//load(path string, dbs DataBaseStruct) (err error)
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
	Path     string
	Ref      map[string]string //словарь - имя коллекции: путь к ней
}

// интерфейс БД
type DataBaseStructInterface interface {
	init() (err error)
	remove(name string) (err error)
}

func (db *DataBaseCollection) init(name string, path string, dbs *DataBaseStruct) error {
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
				db.Name = name
				db.Path = path
				db.ID = dbs.ID_count + 1

				dbs.ID_count = dbs.ID_count + 1
				dbs.Ref[name] = path

				//Создаем или открываем файл со списком коллекций
				db_file, err := os.Create(dbs.Path)
				if err != nil {
					return err
				}

				//Записываем в файл БД название коллекции и путь к ней
				_, err = db_file.WriteString(name + " " + path + "\n")
				if err != nil {
					return err
				}
				defer db_file.Close()

				//Создаем файл для хранения коллекции
				file, err := os.Create(full_path)
				if err != nil {
					return err
				}
				defer file.Close()

			}
		}
	}
	return nil
}

// func (db DataBaseCollection) load(name string, dbs DataBaseStruct) error {
// 	return 0
// }

// удаляем коллекцию из БД
func (dbs DataBaseStruct) remove(name string) error {
	db_path := dbs.Ref[name]
	e := os.Remove(db_path)
	return e
}

// Инициализируем БД
// Для хранения БД испольуется файл с именами коллекций и путями к ним
func (dbs *DataBaseStruct) init(path string) error {
	dbs.Path = path

	_, err_path := os.Stat(path)

	if os.IsNotExist(err_path) {
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		dbs.ID_count = 0
		dbs.Ref = make(map[string]string)

	} else {
		dbs.Ref = make(map[string]string)

		file, _ := os.Open(path)
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanWords)
		for scanner.Scan() {
			name_collection := scanner.Text()
			path_collection := scanner.Text()
			dbs.Ref[name_collection] = path_collection
		}
		dbs.ID_count = len(dbs.Ref)
	}

	return nil
}

func main() {

	dbs := new(DataBaseStruct)
	dbs.init("./database/db_info.txt")

	fmt.Println(*dbs)

	db := new(DataBaseCollection)
	fmt.Println(db.init("test", "./database", dbs))

}
