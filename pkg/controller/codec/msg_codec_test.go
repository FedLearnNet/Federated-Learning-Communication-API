package codec

import (
	"fc_controller/pkg/controller/enums"
	util "fc_controller/pkg/shared/util"
	"math"
	"reflect"
	"testing"
)

// SMPC shards are uniformly random int64 values, they must survive the
// serialization between controllers without any loss of precision.
func TestEncodeDecode__FullRangeInt64Shards(t *testing.T) {
	shards := util.Shard([]util.IntParams{int64(5), []util.IntParams{int64(-7), int64(3)}}, "add", 3, 0)
	shards = append(shards, []util.IntParams{int64(math.MaxInt64), int64(math.MinInt64), int64(0)})
	for _, shard := range shards {
		encoded, err := Encode(shard, nil, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(encoded, nil, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
		if err != nil {
			t.Fatal(err)
		}
		if normalized := util.NormalizeIntParams(decoded); !reflect.DeepEqual(normalized, shard) {
			t.Fatalf("shard changed during serialization: %v != %v", normalized, shard)
		}
	}
}
