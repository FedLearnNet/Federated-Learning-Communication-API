package util

import (
	"encoding/json"
	"math"
	"math/rand"
	"reflect"
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

// runSMPCWorkflow simulates a full SMPC round: every client splits its input
// into one shard per client, every client sums the shards it received and
// the coordinator sums these partial sums.
func runSMPCWorkflow(t *testing.T, inputs []interface{}, operation string, exp int) interface{} {
	t.Helper()
	n := len(inputs)
	received := make([][]IntParams, n)
	for _, input := range inputs {
		intParams, err := PrepareSMPCInput(input, operation, exp, n)
		if err != nil {
			t.Fatalf("PrepareSMPCInput failed: %v", err)
		}
		for i, shard := range Shard(intParams, operation, n, exp) {
			received[i] = append(received[i], shard)
		}
	}
	partialSums := []IntParams{}
	for _, shards := range received {
		partialSums = append(partialSums, Aggregate(shards, operation, exp))
	}
	result, err := FinalizeSMPCResult(Aggregate(partialSums, operation, exp), operation, exp)
	if err != nil {
		t.Fatalf("FinalizeSMPCResult failed: %v", err)
	}
	return result
}

func relativeError(a, b float64) float64 {
	if b == 0 {
		return math.Abs(a)
	}
	return math.Abs(a-b) / math.Abs(b)
}

func TestShardAggregate__Multiply2(t *testing.T) {
	result := runSMPCWorkflow(t, []interface{}{float64(30300), float64(1)}, "multiply", 8)
	if int(math.Round(result.(float64))) != 30300 {
		t.Fatalf("expected 30300, got %v", result)
	}
}

func TestShardAggregate__Multiply10(t *testing.T) {
	inputs := []interface{}{}
	for _, v := range []float64{2, 3, 5, 7, 11, 13, 17, 1, 1, 1} {
		inputs = append(inputs, v)
	}
	result := runSMPCWorkflow(t, inputs, "multiply", 8)
	if int(math.Round(result.(float64))) != 2*3*5*7*11*13*17 {
		t.Fatalf("expected %d, got %v", 2*3*5*7*11*13*17, result)
	}
}

func TestShardAggregate__MultiplySignsZerosAndExtremes(t *testing.T) {
	exp := 10
	inputs := []interface{}{
		testDeserializeJSON([]byte(`[-2.5, 0, 1e-300, 1e150, -1, {"a": 3}]`)),
		testDeserializeJSON([]byte(`[4, 7.5, 1e-5, 1e150, -1, {"a": -0.5}]`)),
		testDeserializeJSON([]byte(`[-0.1, 2, 1e200, 1e-2, -1, {"a": 2}]`)),
	}
	expected := []float64{-2.5 * 4 * -0.1, 0, 1e-300 * 1e-5 * 1e200, 1e150 * 1e150 * 1e-2, -1}
	result := runSMPCWorkflow(t, inputs, "multiply", exp).([]interface{})
	for i, e := range expected {
		got := result[i].(float64)
		if relativeError(got, e) > float64(len(inputs))*math.Pow10(-exp)*10 {
			t.Fatalf("index %d: expected %v, got %v", i, e, got)
		}
	}
	got := result[5].(map[string]interface{})["a"].(float64)
	if relativeError(got, -3) > 1e-8 {
		t.Fatalf("expected -3, got %v", got)
	}
}

func TestShardAggregate__MultiplyManyClients(t *testing.T) {
	n := 500
	exp := 10
	inputs := []interface{}{}
	expected := 1.0
	for i := 0; i < n; i++ {
		v := 0.5 + rand.Float64()
		if i%3 == 0 {
			v = -v
		}
		expected *= v
		inputs = append(inputs, v)
	}
	result := runSMPCWorkflow(t, inputs, "multiply", exp).(float64)
	if relativeError(result, expected) > float64(n)*math.Pow10(-exp) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
}

func TestShardAggregate__MultiplyOverflowFails(t *testing.T) {
	n := 3
	encoded := []IntParams{}
	for i := 0; i < n; i++ {
		e, err := PrepareSMPCInput(1e200, "multiply", 8, n)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, e)
	}
	if _, err := FinalizeSMPCResult(Aggregate(encoded, "multiply", 8), "multiply", 8); err == nil {
		t.Fatal("expected an error for a product exceeding the float64 range")
	}
}

func TestShardAggregate__AddWorkflowManyClients(t *testing.T) {
	for _, n := range []int{1, 2, 3, 10, 50} {
		inputs := []interface{}{}
		expected := 0.0
		for i := 0; i < n; i++ {
			v := math.Round((rand.Float64()*2000-1000)*1e6) / 1e6
			expected += v
			inputs = append(inputs, v)
		}
		result := runSMPCWorkflow(t, inputs, "add", 12).(float64)
		if math.Abs(result-expected) > 1e-6 {
			t.Fatalf("n=%d: expected %v, got %v", n, expected, result)
		}
	}
}

func TestShardAggregate__AddRangeLimits(t *testing.T) {
	for _, n := range []int{2, 3, 10} {
		bound := math.MaxInt64 / int64(n)
		for _, v := range []int64{bound, -bound, 0, 1, -1} {
			if err := CheckSMPCAddRange(v, n); err != nil {
				t.Fatalf("value %d should be accepted for %d clients: %v", v, n, err)
			}
			received := make([][]IntParams, n)
			for c := 0; c < n; c++ {
				for i, shard := range Shard(v, "add", n, 0) {
					received[i] = append(received[i], shard)
				}
			}
			partialSums := make([]IntParams, n)
			for i := range received {
				partialSums[i] = Aggregate(received[i], "add", 0)
			}
			if got := Aggregate(partialSums, "add", 0).(int64); got != v*int64(n) {
				t.Fatalf("n=%d: expected %d, got %d", n, v*int64(n), got)
			}
		}
		if err := CheckSMPCAddRange(bound+1, n); err == nil {
			t.Fatalf("value %d should be rejected for %d clients", bound+1, n)
		}
		if err := CheckSMPCAddRange([]IntParams{int64(0), -bound - 1}, n); err == nil {
			t.Fatalf("value %d should be rejected for %d clients", -bound-1, n)
		}
	}
}

func TestPrepareSMPCInput__RejectsInvalidInputs(t *testing.T) {
	for _, input := range []interface{}{1e10, []interface{}{1.0, 2e7}, math.NaN(), math.Inf(1)} {
		if _, err := PrepareSMPCInput(input, "add", 12, 3); err == nil {
			t.Fatalf("input %v should be rejected at exponent 12", input)
		}
	}
	if _, err := PrepareSMPCInput(math.NaN(), "multiply", 12, 3); err == nil {
		t.Fatal("NaN should be rejected for multiplication")
	}
	if _, err := PrepareSMPCInput([]interface{}{1.0}, "subtract", 12, 3); err == nil {
		t.Fatal("unknown operations should be rejected")
	}
}

func TestShard__SharesAreUniform(t *testing.T) {
	// a small value must be hidden by shards spread over the whole int64 range
	value := FloatToInt(50.0, 12)
	large := 0
	total := 0
	for i := 0; i < 10000; i++ {
		for _, shard := range Shard(value, "add", 3, 12) {
			total++
			s := shard.(int64)
			if s > math.MaxInt64/4 || s < math.MinInt64/4 {
				large++
			}
		}
	}
	// for uniform shards three quarters of them are outside of +-2^61
	if fraction := float64(large) / float64(total); fraction < 0.7 || fraction > 0.8 {
		t.Fatalf("expected about 75%% of the shards outside +-2^61, got %v", fraction)
	}
}

// CBOR decodes integers as int64/uint64 and maps with interface{} keys,
// these must be handled like their float64 and map[string] counterparts
func TestShardAggregate__AddIntegersAndInterfaceMaps(t *testing.T) {
	input := map[interface{}]interface{}{
		"ints":  []interface{}{int64(-3), uint64(2), 1.5},
		"count": uint64(5),
	}
	result := runSMPCWorkflow(t, []interface{}{input, input, input}, "add", 8)
	expected := map[string]interface{}{
		"ints":  []interface{}{float64(-9), float64(6), 4.5},
		"count": float64(15),
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
}

func TestShardAggregate__MultiplyIntegersAndInterfaceMaps(t *testing.T) {
	inputs := []interface{}{
		map[interface{}]interface{}{"a": int64(2)},
		map[interface{}]interface{}{"a": uint64(3)},
		map[interface{}]interface{}{"a": int64(-4)},
	}
	result := runSMPCWorkflow(t, inputs, "multiply", 8).(map[string]interface{})
	if got := result["a"].(float64); relativeError(got, -24) > 1e-7 {
		t.Fatalf("expected about -24, got %v", got)
	}
}

func TestPrepareSMPCInput__RejectsUnsupportedValues(t *testing.T) {
	inputs := []interface{}{
		"x",
		true,
		nil,
		[]interface{}{1.0, "x"},
		map[interface{}]interface{}{1: 1.0},
		map[string]interface{}{"a": []byte{1}},
	}
	for _, operation := range []string{"add", "multiply"} {
		for _, input := range inputs {
			if _, err := PrepareSMPCInput(input, operation, 8, 3); err == nil {
				t.Fatalf("%s: input %v (%T) should be rejected", operation, input, input)
			}
		}
	}
}

func TestPrepareSMPCInput__IntegerRange(t *testing.T) {
	for _, input := range []interface{}{int64(1e12), int64(-1e12), uint64(math.MaxUint64)} {
		if _, err := PrepareSMPCInput(input, "add", 8, 3); err == nil {
			t.Fatalf("input %v should be rejected at exponent 8", input)
		}
	}
	// integers are scaled exactly, also beyond the float64 precision
	large := int64(1)<<60 + 1
	got, err := PrepareSMPCInput(large, "add", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.(int64) != large {
		t.Fatalf("expected %d, got %d", large, got)
	}
}

func TestFloatToInt__Rounds(t *testing.T) {
	// 0.29 * 1e8 is 28999999.999999996 in float64
	if got := FloatToInt(0.29, 8).(int64); got != 29000000 {
		t.Fatalf("expected 29000000, got %d", got)
	}
	got, err := PrepareSMPCInput(-0.29, "add", 8, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.(int64) != -29000000 {
		t.Fatalf("expected -29000000, got %d", got)
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
