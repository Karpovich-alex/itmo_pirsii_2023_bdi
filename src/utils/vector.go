package utils

type Vector struct {
	ID        int
	Embedding []float64
}

func (v Vector) Len() int {
	return len(v.Embedding)
}
