package scrapper

type QueryShapeColumn struct {
	Name       string `json:"name"`
	NativeType string `json:"native_type"`
	Position   int32  `json:"position"`
}
