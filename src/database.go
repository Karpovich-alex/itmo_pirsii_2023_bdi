package main

type DataBase interface {
	init(name string, path string) (err error)
	remove() (err error)

	load() (err error)
	unload() (err error)

	addVector(id int, v Vector) (err error)
	setVector(id int, v Vector) (err error)
	removeVector(id int) (err error)

	findById(id int) (v Vector, err error)
}
