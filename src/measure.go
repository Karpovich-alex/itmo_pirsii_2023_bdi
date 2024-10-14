type Measure interface {
    calc(v1 Vector, v2 Vector) (dist float64, err error)
}