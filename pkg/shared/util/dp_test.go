package util

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestGetWeightsNormTotal(t *testing.T) {
	// generate dummy data
	var dataInterface []interface{} = make([]interface{}, 3)
	var data []float64 = make([]float64, 3)
	dataInterface[0] = rand.Float64()
	dataInterface[1] = int64(rand.Float64() * 7861287)
	// with standard seed, rand.Int() will produce an overflow
	// error when calculating the 2-norm.
	// thats why a float is converted to an int
	dataInterface[2] = -rand.Float64()
	data[0] = dataInterface[0].(float64)
	data[1] = float64(dataInterface[1].(int64))
	data[2] = dataInterface[2].(float64)
	//fmt.Printf("InputData: %v\n", dataInterface)

	jsonData, _ := json.Marshal(dataInterface)
	dataInterface = make([]interface{}, 3)
	_ = json.Unmarshal(jsonData, &dataInterface)

	//normtype 1, count
	weightsNorm, _ := GetWeightsNormTotal(dataInterface, 1)
	correct := data[0] + data[1] - data[2]
	//fmt.Printf("Weightsnorm: %f, correct: %f\n", weightsNorm, correct)
	if weightsNorm != correct {
		t.Fatal()
	}

	//normtype 2
	weightsNorm, _ = GetWeightsNormTotal(dataInterface, 2)
	correct = math.Sqrt(data[0]*data[0] + data[1]*data[1] + data[2]*data[2])
	//fmt.Printf("Weightsnorm: %f, correct: %f\n", weightsNorm, correct)
	if weightsNorm != correct {
		t.Fatal()
	}

	// Overflow check int
	dataInterface[1] = 999999999999999999
	weightsNorm, _ = GetWeightsNormTotal(dataInterface, 2)
	if weightsNorm != math.Inf(1) {
		t.Fatal()
	}

	//Overflow check float
	dataInterface[1] = math.MaxFloat64
	weightsNorm, _ = GetWeightsNormTotal(dataInterface, 2)
	if weightsNorm != math.Inf(1) {
		t.Fatal()
	}
	// This should still work
	weightsNorm, _ = GetWeightsNormTotal(dataInterface[1], 1)
	if weightsNorm == math.Inf(1) {
		t.Fatal()
	}
}

func TestClipValuesGlobal(t *testing.T) {
	dataInterface := make([]interface{}, 3)
	var data []float64 = make([]float64, 3)
	dataInterface[0] = 10.0
	dataInterface[1] = int64(3)
	// with standard seed, rand.Int() will produce an overflow
	// error when calculating the 2-norm.
	// thats why a float is converted to an int
	dataInterface[2] = -7.0
	data[0] = dataInterface[0].(float64)
	data[1] = float64(dataInterface[1].(int64))
	data[2] = dataInterface[2].(float64)

	jsonData, _ := json.Marshal(dataInterface)
	dataInterface = make([]interface{}, 3)
	_ = json.Unmarshal(jsonData, &dataInterface)
	//data:
	//0:float, 1:int, 2:-float

	//TEST1
	// normtype = 1 -> norm is 20, all values should get halved
	clippedDataInterface, sens, err := ClipValuesGlobal(dataInterface, "laplace", 10.0)
	clippedData := make([]interface{}, 3)
	switch clippedDataInterfaceType := clippedDataInterface.(type) {
	case []interface{}:
		for idx, val := range clippedDataInterfaceType {
			clippedData[idx] = val
		}

	}
	if err != nil {
		fmt.Printf("Got unexpected error:\n%s\n", err.Error())
	}

	if sens != (10.0 * 2.0) {
		fmt.Printf("Sensitivity is: %f and should be %f",
			sens, 10.0*2.0)
		t.Fatal()
	}
	for idx, val := range data {
		if clippedData[idx].(float64) != float64(val/2.0) {
			fmt.Printf("%f != %f / 2", clippedData[idx].(float64), val)
			t.Fatal()
		}
	}

	//TEST2
	// normtype = 2
	var norm float64
	for _, val := range data {
		norm += math.Pow(val, 2.0)
	}
	norm = math.Sqrt(norm)

	clippedDataInterface, sens, err = ClipValuesGlobal(dataInterface, "gauss", 10.0)
	switch clippedDataInterfaceType := clippedDataInterface.(type) {
	case []interface{}:
		for idx, val := range clippedDataInterfaceType {
			clippedData[idx] = val
		}

	}
	if err != nil {
		fmt.Printf("Got unexpected error:\n%s\n", err.Error())
	}
	clippingVal := 10.0
	clippingFactor := clippingVal / norm

	if sens != (clippingVal * 2.0) {
		fmt.Printf("Sensitivity is: %f and should be %f",
			sens, 10.0*2.0)
		t.Fatal()
	}

	for idx, val := range data {
		if clippedData[idx].(float64) != float64(val*clippingFactor) {
			fmt.Printf("%f != %f", clippedData[idx].(float64), val*clippingFactor)
			t.Fatal()
		}
	}
}

// TestAddNoise would be good, but as setting a seed does not work, there is no testing
// A more fancy test could include doing many addNoise runs and checking if the distribution of
// the noise applied is the correct one
