package api

import (
	"fmt"

	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/database"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/measures"
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"

	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

var dbs *database.DataBaseStruct

type VectorRequest struct {
	ID     int       `json:"id"`
	Vector []float64 `json:"vector"`
}

func CreateOrGetDB(w http.ResponseWriter, r *http.Request) {

	var err error

	name := r.URL.Query().Get("name")
	path := r.URL.Query().Get("path")

	dbs, err = database.NewDataBase(name, path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(dbs)

	fmt.Println(dbs)

}

func CreateCollection(w http.ResponseWriter, r *http.Request) {

	name := r.URL.Query().Get("name")

	err := dbs.AddCollection(name)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println(dbs)
}

func DeleteCollection(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]

	err := dbs.RemoveCollection(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println(dbs)
}

func LoadCollection(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]

	err := dbs.Load(name)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println(dbs)
}

func AddVector(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]

	var vectorRequest VectorRequest

	err := json.NewDecoder(r.Body).Decode(&vectorRequest)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	v := &utils.Vector{ID: vectorRequest.ID, Embedding: vectorRequest.Vector}

	err = dbs.AddVector(name, v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func FlushCollection(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]

	err := dbs.Flush(name)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func RemoveVector(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]
	id_str := vars["id"]

	id, err := strconv.Atoi(id_str)
	if err != nil {
		http.Error(w, "Invalid vector id", http.StatusBadRequest)
		return
	}

	err = dbs.RemoveVector(name, id)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func GetVector(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]
	id_str := vars["id"]

	id, err := strconv.Atoi(id_str)
	if err != nil {
		http.Error(w, "Invalid vector id", http.StatusBadRequest)
		return
	}

	v, err := dbs.FindById(name, id)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(v)

}

func GetClosest(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]
	//measure_str := vars["measure"]

	n_str := r.URL.Query().Get("n")

	n, err := strconv.Atoi(n_str)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var vectorRequest VectorRequest

	err = json.NewDecoder(r.Body).Decode(&vectorRequest)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	vector := &utils.Vector{ID: vectorRequest.ID, Embedding: vectorRequest.Vector}

	//TODO for cosine
	results, err := dbs.FindClosest(name, vector, measures.EuclideanDistanceMeasure{}, n)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(results)

}

func UpdateVector(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	name := vars["name"]

	var vectorRequest VectorRequest

	err := json.NewDecoder(r.Body).Decode(&vectorRequest)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	v := &utils.Vector{ID: vectorRequest.ID, Embedding: vectorRequest.Vector}

	err = dbs.UpdateVector(name, v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}
