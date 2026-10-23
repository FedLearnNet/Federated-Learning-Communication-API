package codec

import (
	"fc_controller/pkg/controller/enums"
	util "fc_controller/pkg/shared/util"
	"math"
	"reflect"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// SMPC shards are uniformly random int64 values, they must survive the
// serialization between controllers without any loss of precision.
func TestEncodeDecode__FullRangeInt64Shards(t *testing.T) {
	shards := util.Shard([]util.IntParams{int64(5), []util.IntParams{int64(-7), int64(3)}}, "add", 3, 0)
	shards = append(shards, util.Shard(map[string]util.IntParams{"a": int64(5), "b": []util.IntParams{int64(-7)}}, "add", 3, 0)...)
	shards = append(shards, []util.IntParams{int64(math.MaxInt64), int64(math.MinInt64), int64(0)})
	for _, shard := range shards {
		if normalized := encodeDecodeShard(t, shard); !reflect.DeepEqual(normalized, shard) {
			t.Fatalf("shard changed during serialization: %v != %v", normalized, shard)
		}
	}
}

// An app payload sent as CBOR contains integers and maps with interface{}
// keys, the full SMPC round including the serialization of shards and
// partial sums between controllers must handle these.
func TestSMPCWorkflow__CBORAppPayload(t *testing.T) {
	n := 3
	exp := 8
	payload, err := cbor.Marshal(map[string]interface{}{
		"floats": []interface{}{1.5, -2.25},
		"ints":   []interface{}{2, -3},
		"count":  5,
	})
	if err != nil {
		t.Fatal(err)
	}

	received := make([][]util.IntParams, n)
	for c := 0; c < n; c++ {
		data, err := Decode(payload, nil, enums.SerializationCBOR, enums.CompressionNone)
		if err != nil {
			t.Fatal(err)
		}
		intParams, err := util.PrepareSMPCInput(data, "add", exp, n)
		if err != nil {
			t.Fatal(err)
		}
		for i, shard := range util.Shard(intParams, "add", n, exp) {
			received[i] = append(received[i], encodeDecodeShard(t, shard))
		}
	}
	partialSums := []util.IntParams{}
	for _, shards := range received {
		partialSums = append(partialSums, encodeDecodeShard(t, util.Aggregate(shards, "add", exp)))
	}
	result, err := util.FinalizeSMPCResult(util.Aggregate(partialSums, "add", exp), "add", exp)
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]interface{}{
		"floats": []interface{}{4.5, -6.75},
		"ints":   []interface{}{float64(6), float64(-9)},
		"count":  float64(15),
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
}

// encodeDecodeShard serializes a shard the same way as it is sent between
// controllers and normalizes the decoded data like the receiving controller
func encodeDecodeShard(t *testing.T, shard util.IntParams) util.IntParams {
	t.Helper()
	encoded, err := Encode(shard, nil, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded, nil, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
	if err != nil {
		t.Fatal(err)
	}
	return util.NormalizeIntParams(decoded)
}
