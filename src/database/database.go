package database

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/index"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/measures"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
)

// интерфейс коллекции
type DataBase interface {
	Init(name string, path string, databasestruct DataBaseStruct) (err error)

	Load(name string, dbs DataBaseStruct) (err error)
	Flush(name string, dbs DataBaseStruct) (err error)

	AddVector(v utils.Vector) (err error)
	SetVector(id int, v utils.Vector) (err error)
	RemoveVector(id int) (err error)

	//FindById(id int) (v utils.Vector, err error)
	//FindClosest(v utils.Vector, measure measures.Measure, n int) (results []*index.SearchResult, err error)

	FindClosest(v *utils.Vector, measure measures.Measure, n int) (results []*index.SearchResult)
	FindById(id int) (v *utils.Vector, err error)
}

// одна коллекция в БД
type DataBaseCollection struct {
	ID     int
	Name   string
	Path   string
	Vector []utils.Vector
	Index  index.FlatIndex
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

func (db *DataBaseCollection) Init(name string, path string, dbs *DataBaseStruct) error {
	info_path, err_path := os.Stat(path)
	if os.IsNotExist(err_path) {
		return errors.New("Path doesn't exist!")
	} else {
		if !info_path.IsDir() {
			return errors.New("Path should point to the directory!")
		} else {
			full_path := path + "/" + name + ".txt"

			_, err_file := os.Stat(full_path)
			if os.IsNotExist(err_file) {

				dbs.ID_count = dbs.ID_count + 1
				dbs.Ref[name] = path

				//Создаем или открываем файл со списком коллекций
				db_file, err := os.OpenFile(dbs.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
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
			db.Name = name
			db.Path = path
			db.ID = dbs.ID_count + 1
			db.Vector = make([]utils.Vector, 0)
			db.Index = index.FlatIndex{}
		}
	}
	return nil
}

func (db *DataBaseCollection) Load(name string, dbs *DataBaseStruct) error {

	db_path := dbs.Ref[name] + "/" + name + ".txt"

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
	defer file.Close()

	err := db.Index.Load("./database/index.txt")
	if err != nil {
		panic(err)
	}

	return nil

}

func (db *DataBaseCollection) Flush(name string, dbs *DataBaseStruct) error {
	db_path := dbs.Ref[name] + "/" + name + ".txt"

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
			str := strconv.FormatFloat(v, 'f', -1, 64)
			str_vect = append(str_vect, str)
		}

		line := strings.Join(str_vect, " ")

		_, err := writer.WriteString(line + "\n")
		if err != nil {
			return err
		}

		if err := writer.Flush(); err != nil {
			return err
		}

		err = db.Index.Flush("./database/index.txt")
		if err != nil {
			panic(err)
		}
	}

	defer file.Close()

	return nil
}

func (db *DataBaseCollection) AddVector(v utils.Vector) (err error) {

	db.Vector = append(db.Vector, v)
	db.Index.AddVector(&v)

	return nil
}

func (db *DataBaseCollection) SetVector(id int, v utils.Vector) (err error) {
	db.Vector[id] = v
	return nil
}

func (db *DataBaseCollection) RemoveVector(id int) (err error) {
	if id < 0 || id > len(db.Vector) {
		return errors.New("Id is inccorect!")
	}
	db.Vector = slices.Delete(db.Vector, id, id+1)

	db.Index.RemoveVector(id)
	return nil
}

func (db *DataBaseCollection) FindById(id int) (v *utils.Vector, err error) {
	return db.Index.FindById(id)
}

func (db *DataBaseCollection) FindClosest(v *utils.Vector, measure measures.Measure, n int) (results []*index.SearchResult) {
	return db.Index.FindClosest(v, measure, n)
}

// удаляем коллекцию из БД
func (dbs *DataBaseStruct) Remove(name string) error {
	db_path := dbs.Ref[name] + "/" + name + ".txt"

	e := os.Remove(db_path)
	if e != nil {
		return e
	}
	delete(dbs.Ref, name)

	fmt.Println(dbs.Ref)

	file, err := os.OpenFile("./source/db_info.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for n, p := range dbs.Ref {
		_, err = file.WriteString(n + " " + p + "\n")
		if err != nil {
			return err
		}
	}

	return e
}

// Инициализируем БД
// Для хранения БД испольуется файл с именами коллекций и путями к ним
func (dbs *DataBaseStruct) Init(path string) error {
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
		for scanner.Scan() {
			line := scanner.Text()
			str_emb := strings.Fields(line)
			name_collection := str_emb[0]
			path_collection := str_emb[1]
			dbs.Ref[name_collection] = path_collection

		}

		dbs.ID_count = len(dbs.Ref)
	}

	return nil
}
