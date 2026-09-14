package util

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"
)

func testDeserializeJSON(bts []byte) interface{} {
	var params interface{}
	_ = json.Unmarshal(bts, &params)
	return params
}

func Test_restructureAggregate1(t *testing.T) {
	paramsInt := []IntParams{int64(1), int64(5)}

	ps := restructureAggregate(paramsInt)

	if !intsEqual(paramsInt, ps) {
		t.Fatal()
	}
}

func TestShardAggregate1(t *testing.T) {
	paramsInt := FloatToInt(float64(5), 8)

	ps := Shard(paramsInt, "add", 5, 8)

	if len(ps) != 5 {
		t.Fatal()
	}

	params2 := Aggregate(ps, "add", 8)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate2(t *testing.T) {
	params := testDeserializeJSON([]byte("[5,2,7]"))
	paramsInt := FloatToInt(params, 8)

	ps := Shard(paramsInt, "add", 5, 7)

	if len(ps) != 5 {
		t.Fatal()
	}

	params2 := Aggregate(ps, "add", 7)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate3(t *testing.T) {
	astr := "{\"a\":5,\"b\":2}"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)

	ps := Shard(paramsInt, "add", 5, 5)

	if len(ps) != 5 {
		t.Fatal()
	}

	params2 := Aggregate(ps, "add", 5)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate4(t *testing.T) {
	astr := "[[1]]"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)

	ps := Shard(paramsInt, "add", 5, 5)

	if len(ps) != 5 {
		t.Fatal()
	}

	params2 := Aggregate(ps, "add", 5)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate5(t *testing.T) {
	astr := "{\"a\":{\"b\":1}}"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)
	ps := Shard(paramsInt, "add", 5, 8)

	ps = append(ps, nil, nil, nil)
	ps[3], ps[5] = ps[5], ps[3]
	ps[0], ps[6] = ps[6], ps[0]

	params2 := Aggregate(ps, "add", 8)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate6(t *testing.T) {
	astr := "{\"a\":[1]}"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)
	ps := Shard(paramsInt, "add", 5, 8)

	ps = append(ps, nil, nil, nil)
	ps[3], ps[5] = ps[5], ps[3]
	ps[0], ps[6] = ps[6], ps[0]

	params2 := Aggregate(ps, "add", 8)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate7(t *testing.T) {
	astr := "[{\"a\":1}]"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)
	ps := Shard(paramsInt, "add", 5, 5)

	ps = append(ps, nil, nil, nil)
	ps[3], ps[5] = ps[5], ps[3]
	ps[0], ps[6] = ps[6], ps[0]

	params2 := Aggregate(ps, "add", 5)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate8(t *testing.T) {
	astr := "[1,[2,3]]"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)
	ps := Shard(paramsInt, "add", 5, 5)

	ps = append(ps, nil, nil, nil)
	ps[3], ps[5] = ps[5], ps[3]
	ps[0], ps[6] = ps[6], ps[0]

	params2 := Aggregate(ps, "add", 5)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate9(t *testing.T) {
	astr := "null"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)
	ps := Shard(paramsInt, "add", 5, 5)

	ps = append(ps, nil, nil, nil)
	ps[3], ps[5] = ps[5], ps[3]
	ps[0], ps[6] = ps[6], ps[0]

	params2 := Aggregate(ps, "add", 5)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate10(t *testing.T) {
	astr := "[null,5,{\"a\":1,\"b\":null},null,5]"

	params := testDeserializeJSON([]byte(astr))
	paramsInt := FloatToInt(params, 8)

	ps := Shard(paramsInt, "add", 5, 8)

	ps = append(ps, nil, nil, nil)
	ps[3], ps[5] = ps[5], ps[3]
	ps[0], ps[6] = ps[6], ps[0]

	params2 := Aggregate(ps, "add", 8)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate__AddLarge(t *testing.T) {
	n := 500
	max := 1000.0
	exp := 10

	params := []interface{}{}
	for i := 0; i < 2000; i++ {
		params = append(params, float64(int64(rand.Float64()*max))/max)
	}

	paramsInt := FloatToInt(params, 8)
	ps := Shard(paramsInt, "add", n, exp)

	if len(ps) != n {
		t.Fatal()
	}

	params2 := Aggregate(ps, "add", exp)
	if !intsEqual(paramsInt, params2) {
		t.Fatal()
	}
}

func TestShardAggregate__Multiply2(t *testing.T) {
	astrInt := 30300
	exp := 8 // With a big exponent we have the risk of overflowing

	paramsInt := FloatToInt(float64(astrInt), exp)
	ps := Shard(paramsInt, "multiply", 2, exp)
	if len(ps) != 2 {
		t.Fatal()
	}

	params2 := Aggregate(ps, "multiply", exp)
	result := IntToFloat(params2, exp)
	if int(math.Round(result.(float64))) != astrInt {
		t.Fatal()
	}
}

func TestShardAggregate__Multiply10(t *testing.T) {
	astrInt := 2 * 3 * 5 * 7 * 11 * 13 * 17
	exp := 8
	paramsInt := FloatToInt(float64(astrInt), exp)
	ps := Shard(paramsInt, "multiply", 10, exp)
	if len(ps) != 10 {
		t.Fatal()
	}

	params2 := Aggregate(ps, "multiply", exp)
	result := IntToFloat(params2, exp)
	if int(math.Round(result.(float64))) != astrInt {
		t.Fatal()
	}
}

func TestRescale(t *testing.T) {
	params := map[string]interface{}{
		"a": float64(123),
		"b": []interface{}{
			float64(-45),
			map[string]interface{}{"c": float64(7)},
		},
	}

	rescaled := Rescale(params, 2, 4)

	expected := map[string]IntParams{
		"a": int64(12300),
		"b": []IntParams{
			int64(-4500),
			map[string]IntParams{"c": int64(700)},
		},
	}

	if !intsEqual(rescaled, expected) {
		t.Fatal()
	}
}

func TestRescale_Int64(t *testing.T) {
	result := Rescale(int64(5), 2, 4)
	if result.(int64) != int64(500) {
		t.Fatal()
	}
}

func TestRescale_SameExponent(t *testing.T) {
	params := []IntParams{int64(100), int64(200)}
	result := Rescale(params, 3, 3)
	if !intsEqual(params, result) {
		t.Fatal()
	}
}

func TestRescale_Downscale(t *testing.T) {
	result := Rescale(int64(12300), 4, 2)
	if result.(int64) != int64(123) {
		t.Fatal()
	}
}

func TestShardWorkflow1(t *testing.T) {
	exp := 10

	pi1 := FloatToInt(float64(2), exp)
	pi2 := FloatToInt(float64(8), exp)

	s1 := Shard(pi1, "add", 2, exp)
	s2 := Shard(pi2, "add", 2, exp)

	sa1 := []IntParams{s1[0], s2[1]}
	sa2 := []IntParams{s1[1], s2[0]}

	a1 := Aggregate(sa1, "add", exp)
	a2 := Aggregate(sa2, "add", exp)

	a := Aggregate([]IntParams{a1, a2}, "add", exp)

	result := IntToFloat(a, exp)
	if result.(float64) != 10.0 {
		t.Fatal()
	}
}

func TestShardWorkflow12(t *testing.T) {
	exp := 0

	pi1 := FloatToInt(float64(2), exp)
	pi2 := FloatToInt(float64(8), exp)

	s1 := Shard(pi1, "add", 2, exp)
	s2 := Shard(pi2, "add", 2, exp)

	sa1 := []IntParams{s1[0], s2[1]}
	sa2 := []IntParams{s1[1], s2[0]}

	a1 := Aggregate(sa1, "add", exp)
	a2 := Aggregate(sa2, "add", exp)

	a := Aggregate([]IntParams{a1, a2}, "add", exp)

	result := IntToFloat(a, exp)
	if result.(float64) != 10.0 {
		t.Fatal()
	}
}

/*func TestShardAggregate__MultiplyLarge(t *testing.T) {
	rand.Seed(1)
	n := 2
	l := 16
	max := 1.0
	exp := 12

	params := []interface{}{}
	for i := 0; i < l; i++ {
		params = append(params, float64(int(rand.Float64()*max*16))/16)
	}

	paramsInt := FloatToInt(params, exp)
	ps := Shard(paramsInt, "multiply", n, exp)

	if len(ps) != n {
		t.Fatal()
	}

	params2 := Aggregate(ps, "multiply", exp)
	result := IntToFloat(params2, exp)
	result2 := IntToFloat(paramsInt, exp)
	if result != result2 {
		t.Fatal()
	}
}*/
