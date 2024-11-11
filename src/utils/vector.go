package utils

type Vector struct {
	ID        int
	Embedding []float64
}

func (v Vector) Len() int {
	return len(v.Embedding)
}

func (v Vector) Copy() Vector {
	newId := v.ID
	newEmbedding := make([]float64, len(v.Embedding))

	for i := range v.Embedding {
		newEmbedding[i] = v.Embedding[i]
	}

	vector := Vector{ID: newId, Embedding: newEmbedding}
	return vector
}
