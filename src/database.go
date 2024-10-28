package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
	"os"
	"slices"
	"strconv"
	"strings"
)

// интерфейс коллекции
type DataBase interface {
	init(name string, path string, databasestruct DataBaseStruct) (err error)

	load(name string, dbs DataBaseStruct) (err error)
	flush(name string, dbs DataBaseStruct) (err error)

	addVector(v utils.Vector) (err error)
	setVector(id int, v utils.Vector) (err error)
	removeVector(id int) (err error)

	findById(id int) (v utils.Vector, err error)
	// findClosest(v utils.Vector, measure Measure, n int) (results []SearchResult, err error)
}

// одна коллекция в БД
type DataBaseCollection struct {
	ID     int
	Name   string
	Path   string
	Vector []utils.Vector
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
				db.Vector = make([]utils.Vector, 0)

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

func (db *DataBaseCollection) load(name string, dbs *DataBaseStruct) error {

	db_path := dbs.Ref[name]

	file, _ := os.Open(db_path)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		str_emb := strings.Fields(line)

		var float_emb []float64
		for _, str := range str_emb {
			if value, err := strconv.ParseFloat(str, 64); err == nil {
				float_emb = append(float_emb, value)
			} else {
				return err
			}
		}

		vect := new(utils.Vector)

		count := len(db.Vector)

		vect.ID = count + 1
		vect.Embedding = float_emb

		db.Vector = append(db.Vector, *vect)

	}

	return nil

}

func (db *DataBaseCollection) flush(name string, dbs *DataBaseStruct) error {
	db_path := dbs.Ref[name]

	if _, err := os.Stat(db_path); err == nil {
		err := os.Remove(db_path)
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	file, err := os.Create(db_path)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(file)

	for _, vect := range db.Vector {

		var str_vect []string
		for _, v := range vect.Embedding {
			// Convert float to string with specified format and precision
			str := strconv.FormatFloat(v, 'f', -1, 64) // 'f' for decimal point notation
			str_vect = append(str_vect, str)
		}

		line := strings.Join(str_vect, " ")

		_, err := writer.WriteString(line + "\n") // Append newline character
		if err != nil {
			return err
		}
	}

	defer file.Close()

	return nil
}

func (db *DataBaseCollection) addVector(v utils.Vector) (err error) {
	db.Vector = append(db.Vector, v)
	return nil
}

func (db *DataBaseCollection) setVector(id int, v utils.Vector) (err error) {
	db.Vector[id] = v
	return nil
}

func (db *DataBaseCollection) removeVector(id int) (err error) {
	if id < 0 || id > len(db.Vector) {
		return errors.New("Id is inccorect!")
	}
	db.Vector = slices.Delete(db.Vector, id, id+1)
	return nil
}

func (db *DataBaseCollection) findById(id int) (res *utils.Vector, err error) {
	if id < 0 || id > len(db.Vector) {
		return nil, errors.New("Id is inccorect!")
	}
	return &db.Vector[id], nil
}

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
	dbs.init("./src/database/db_info.txt")

	db := new(DataBaseCollection)
	fmt.Println(db.init("test", "./database", dbs))

}
