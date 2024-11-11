package measures

import (
	"github.com/karpovich-alex/itmo_pirsii_2023_bdi/src/utils"
	"math"
)

type Measure interface {
	Calc(v1 utils.Vector, v2 utils.Vector) (dist float64)
}

type CosineDistanceMeasure struct{}

func (m CosineDistanceMeasure) Calc(v1 utils.Vector, v2 utils.Vector) (dist float64) {
	if v1.Len() != v2.Len() || v1.Len() == 0 {
		return 0.0
	}

	dotProduct := 0.0
	magA := 0.0
	magB := 0.0

	for i := 0; i < v1.Len(); i++ {
		dotProduct += v1.Embedding[i] * v2.Embedding[i]
		magA += v1.Embedding[i] * v1.Embedding[i]
		magB += v2.Embedding[i] * v2.Embedding[i]
	}

	magA = math.Sqrt(magA)
	magB = math.Sqrt(magB)

	if magA == 0 || magB == 0 {
		return 0.0
	}

	return -dotProduct / (magA * magB)
}

type EuclideanDistanceMeasure struct{}

func (m EuclideanDistanceMeasure) Calc(v1 utils.Vector, v2 utils.Vector) (dist float64) {
	if v1.Len() != v2.Len() || v1.Len() == 0 {
		return 0.0
	}

	sum := 0.0

	for i := 0; i < v1.Len(); i++ {
		diff := v1.Embedding[i] - v2.Embedding[i]
		sum += diff * diff
	}

	return math.Sqrt(sum)
}

//func main() {
//	c := new(CosineDistanceMeasure)
//	e := new(EuclideanDistanceMeasure)
//
//	v1 := utils.Vector{1, []float64{0.1, 0.2, 0.3, 0.4}}
//	v2 := utils.Vector{2, []float64{0.1, 0.2, 0.3, 0.4}}
//
//	dist := c.Calc(v1, v2)
//	fmt.Println(dist)
//	dist = e.Calc(v1, v2)
//	fmt.Println(dist)
//
//}
