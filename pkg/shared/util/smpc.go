package util

import (
	"crypto/rand"
	"fc_controller/pkg/shared/logger"
	"math"
	"math/big"
	mrand "math/rand"
)

// IntParams contains parameters encoded as fixed point numbers
type IntParams interface{}

const SMPC = "SMPC"

// NormalizeIntParams converts CBOR-decoded types to proper IntParams types.
// After CBOR round-trip, positive integers decode as uint64 and slices as
// []interface{} instead of []IntParams. This normalizes them so aggregation
// type assertions match correctly.
func NormalizeIntParams(v interface{}) IntParams {
	switch p := v.(type) {
	case []interface{}:
		arr := make([]IntParams, len(p))
		for i, e := range p {
			arr[i] = NormalizeIntParams(e)
		}
		return arr
	case uint64:
		return int64(p)
	case map[string]interface{}:
		m := make(map[string]IntParams, len(p))
		for k, e := range p {
			m[k] = NormalizeIntParams(e)
		}
		return m
	default:
		return p
	}
}

// FloatToInt converts float64 parameters into int64 parameters
// Keeps the same structure as params
func FloatToInt(params interface{}, exp int) IntParams {
	switch p := params.(type) {
	case []interface{}:
		arr := []IntParams{}
		for _, i := range p {
			arr = append(arr, FloatToInt(i, exp))
		}
		return arr

	case map[string]interface{}:
		arr := map[string]IntParams{}
		for k, i := range p {
			arr[k] = FloatToInt(i, exp)
		}
		return arr

	case float64:
		return toInt64(p, exp)

	default:
		return p
	}
}

// IntToFloat converts int64 parameters into float64 parameters
// Keeps the same structure as params
func IntToFloat(params IntParams, exp int) interface{} {
	switch p := params.(type) {
	case []IntParams:
		arr := []interface{}{}
		for _, i := range p {
			arr = append(arr, IntToFloat(i, exp))
		}
		return arr

	case map[string]IntParams:
		arr := map[string]interface{}{}
		for k, i := range p {
			arr[k] = IntToFloat(i, exp)
		}
		return arr

	case int64:
		return fromInt64(p, exp)

	default:
		return p
	}
}

// Rescale converts fixed-point parameters from sourceExp to targetExp.
// It preserves the nested structure and only rescales int64 leaves.
func Rescale(params IntParams, sourceExp uint8, targetExp uint8) IntParams {
	if sourceExp == targetExp {
		return params
	}

	switch p := params.(type) {
	case []interface{}:
		arr := make([]IntParams, len(p))
		for i, v := range p {
			arr[i] = Rescale(v, sourceExp, targetExp)
		}
		return arr

	case map[string]interface{}:
		arr := map[string]IntParams{}
		for k, v := range p {
			arr[k] = Rescale(v, sourceExp, targetExp)
		}
		return arr

	case []IntParams:
		arr := make([]IntParams, len(p))
		for i, v := range p {
			arr[i] = Rescale(v, sourceExp, targetExp)
		}
		return arr

	case []int64:
		arr := make([]IntParams, len(p))
		for i, v := range p {
			arr[i] = rescaleInt64(v, sourceExp, targetExp)
		}
		return arr

	case map[string]IntParams:
		arr := map[string]IntParams{}
		for k, v := range p {
			arr[k] = Rescale(v, sourceExp, targetExp)
		}
		return arr

	case float64:
		return rescaleInt64(int64(math.Round(p)), sourceExp, targetExp)

	case int64:
		return rescaleInt64(p, sourceExp, targetExp)

	case uint64:
		return rescaleInt64(int64(p), sourceExp, targetExp)

	default:
		return p
	}
}

func rescaleInt64(value int64, sourceExp uint8, targetExp uint8) int64 {
	if sourceExp == targetExp {
		return value
	}

	delta := int(targetExp) - int(sourceExp)
	absDelta := delta
	if absDelta < 0 {
		absDelta = -absDelta
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(absDelta)), nil)
	rescaled := big.NewInt(value)

	if delta > 0 {
		rescaled.Mul(rescaled, factor)
	} else {
		logger.Warn(SMPC, "", "Downscaling from exponenet %d to %d, this will lead to increased noise.", sourceExp, targetExp)
		rescaled.Quo(rescaled, factor)
	}

	if !rescaled.IsInt64() {
		logger.Fatal(SMPC, "", "INTEGER OVERFLOW/UNDERFLOW IN SMPC RESCALE")
	}

	return rescaled.Int64()
}

// // Converts string parameters marked with the prefix "i64:" into int64 parameters,
// // Keeps the same structure as params
// // Needed as JSON serialization serializes ints to float
// func StringToInt(params interface{}) IntParams {
// 	switch p := params.(type) {
// 	case []interface{}:
// 		arr := []IntParams{}
// 		for _, i := range p {
// 			arr = append(arr, StringToInt(i))
// 		}
// 		return arr

// 	case map[string]interface{}:
// 		arr := map[string]IntParams{}
// 		for k, i := range p {
// 			arr[k] = StringToInt(i)
// 		}
// 		return arr

// 	case string:
// 		if strings.HasPrefix(p, "i64:") {
// 			i, _ := strconv.ParseInt(p[4:], 10, 64)
// 			return i
// 		}
// 		return p

// 	default:
// 		return p
// 	}
// }

// // Converts int64 parameters marked into string parameters with the prefix "i64:",
// // Keeps the same structure as params
// // Needed as JSON serialization serializes ints to float
// func IntToString(params IntParams) interface{} {
// 	switch p := params.(type) {
// 	case []IntParams:
// 		arr := []interface{}{}
// 		for _, i := range p {
// 			arr = append(arr, IntToString(i))
// 		}
// 		return arr

// 	case map[string]IntParams:
// 		arr := map[string]interface{}{}
// 		for k, i := range p {
// 			arr[k] = IntToString(i)
// 		}
// 		return arr

// 	case int64:
// 		return fmt.Sprintf("i64:%d", p)

// 	default:
// 		return p
// 	}
// }

// Shard creates n shards based on the specified operation
func Shard(params IntParams, operation string, n int, exp int) []IntParams {
	// params = toInt64All(params, exp)

	ps := []IntParams{}

	switch p := params.(type) {
	case []IntParams:
		for i := 0; i < n; i++ {
			ps = append(ps, []IntParams{})
		}
		for _, pj := range p {
			pd := Shard(pj, operation, n, exp)
			for i := range ps {
				ps[i] = append(ps[i].([]IntParams), pd[i])
			}
		}

	case map[string]IntParams:
		for i := 0; i < n; i++ {
			ps = append(ps, map[string]IntParams{})
		}
		for pk, pj := range p {
			pd := Shard(pj, operation, n, exp)
			for i := range ps {
				mp := ps[i].(map[string]IntParams)
				mp[pk] = pd[i]
				ps[i] = mp
			}
		}

	case int64:
		switch operation {
		case "add":
			rs := big.NewInt(0)
			for i := 0; i < n-1; i++ {
				r, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64>>20))
				if err != nil {
					logger.Fatal(SMPC, "", "Error in shard generation: %s", err)
				}
				// Since this only gives positive numbers we need to randomly
				// make the numbers negative
				rbool, err := rand.Int(rand.Reader, big.NewInt(2))
				if err != nil {
					logger.Fatal(SMPC, "", "Error in shard generation: %s", err)
				}
				if len(rbool.Bytes()) == 0 {
					// if rbool is 0, the bytes representation is 0 bytes
					r.Neg(r)
				}
				rs.Add(rs, r)
				ps = append(ps, r.Int64())
			}
			// check for overflow
			lastShardInt := big.NewInt(0)
			lastShardInt = lastShardInt.Sub(big.NewInt(p), rs)
			if !lastShardInt.IsInt64() || !rs.IsInt64() {
				logger.Fatal(SMPC, "", "INTEGER OVERFLOW/UNDERFLOW IN SMPC ADDITION")
			}
			ps = append(ps, lastShardInt.Int64())
			arrayShuffle(ps)

		case "multiply":
			rs := 1.0
			for i := 0; i < n-1; i++ {
				rf := mrand.Float64()
				if rs < 1.0 {
					rf = 1 / rf
				}
				ri := toInt64(rf, exp)
				ps = append(ps, ri)
				rs *= fromInt64(ri, exp)
			}
			ps = append(ps, toInt64(fromInt64(p, exp)/rs, exp))
			mrand.Shuffle(n, func(i, j int) {
				ps[i], ps[j] = ps[j], ps[i]
			})

		default:
			logger.Fatal(SMPC, "", "Nonimplemented case for SMPC, unsupported datatype %T", p)
			return nil
		}

	case string:
		logger.Fatal(SMPC, "", "Value type error: %s. Expecting number or array of numbers", p)

	default:
		ps = []IntParams{}
		for i := 0; i < n; i++ {
			ps = append(ps, nil)
		}
	}

	return ps
}

// toInt64 transforms a float64 into an int64
func toInt64(f float64, exp int) int64 {
	return int64(f * math.Pow10(exp))
}

// fromInt64 transforms an int64 into a float64
func fromInt64(i int64, exp int) float64 {
	return float64(i) / math.Pow10(exp)
}

// Aggregate aggregates parameters into a new parameter struct based on the specified operation
func Aggregate(params []IntParams, operation string, exp int) IntParams {
	agg := restructureAggregate(params)
	sum := applyOperation(agg, operation, exp)

	return sum
}

func restructureAggregate(params []IntParams) interface{} {
	var s IntParams

	for _, p := range params {
		switch pv := p.(type) {
		case []IntParams:
			if s == nil {
				s = []IntParams{}
			} else if _, ok := s.([]IntParams); !ok {
				continue
			}
			for pk, pe := range pv {
				if pf, ok := pe.(int64); ok {
					if len(s.([]IntParams)) > pk {
						s.([]IntParams)[pk] = append(s.([]IntParams)[pk].([]IntParams), pf)
					} else {
						s = append(s.([]IntParams), []IntParams{pf})
					}
				} else {
					if len(s.([]IntParams)) > pk {
						s.([]IntParams)[pk] = append(s.([]IntParams)[pk].([]IntParams), pe)
					} else {
						s = append(s.([]IntParams), []IntParams{pe})
					}
				}
			}

		case map[string]IntParams:
			if s == nil {
				s = map[string]IntParams{}
			} else if _, ok := s.(map[string]IntParams); !ok {
				continue
			}
			for pk, pe := range pv {
				if pf, ok := pe.(int64); ok {
					if _, ok := s.(map[string]IntParams)[pk]; !ok {
						s.(map[string]IntParams)[pk] = []IntParams{pf}
					} else {
						s.(map[string]IntParams)[pk] = append(s.(map[string]IntParams)[pk].([]IntParams), pf)
					}
				} else {
					if _, ok := s.(map[string]IntParams)[pk]; !ok {
						s.(map[string]IntParams)[pk] = []IntParams{pe}
					} else {
						s.(map[string]IntParams)[pk] = append(s.(map[string]IntParams)[pk].([]IntParams), pe)
					}
				}
			}

		case int64:
			if s == nil {
				s = []int64{}
			} else if _, ok := s.([]int64); !ok {
				continue
			}
			s = append(s.([]int64), pv)
		}
	}

	switch sv := s.(type) {
	case []IntParams:
		for pk, pe := range sv {
			if pf, ok := pe.([]IntParams); ok {
				sv[pk] = restructureAggregate(pf)
			}
		}

	case map[string]IntParams:
		for pk, pe := range sv {
			if pf, ok := pe.([]IntParams); ok {
				sv[pk] = restructureAggregate(pf)
			}
		}
	}

	return s
}

func applyOperation(params IntParams, operation string, exp int) IntParams {
	if operation != "add" && operation != "multiply" {
		return nil
	}

	var s interface{}

	switch pv := params.(type) {
	case []int64:
		switch operation {
		case "add":
			s = int64(0)
			for _, pe := range pv {
				s = saveAdd(s.(int64), pe)
			}
		case "multiply":
			s = float64(1)
			for _, pe := range pv {
				s = s.(float64) * fromInt64(pe, exp)
			}
			s = toInt64(s.(float64), exp)
		default:
			return nil
		}

	case []IntParams:
		s = []IntParams{}
		for _, pe := range pv {
			s = append(s.([]IntParams), applyOperation(pe, operation, exp))
		}

	case map[string]IntParams:
		s = map[string]IntParams{}
		for pk, pe := range pv {
			s.(map[string]IntParams)[pk] = applyOperation(pe, operation, exp)
		}
	}

	return s
}

func intsEqual(i1, i2 IntParams) bool {
	if i1 == nil && i2 == nil {
		return true
	}
	switch p := i1.(type) {
	case []IntParams:
		switch p2 := i2.(type) {
		case []IntParams:
			for pi, pj := range p {
				if !intsEqual(pj, p2[pi]) {
					return false
				}
			}
			return true
		case []int64:
			for pi, pj := range p {
				if !intsEqual(pj, p2[pi]) {
					return false
				}
			}
			return true
		}
		return false

	case []int64:
		switch p2 := i2.(type) {
		case []IntParams:
			for pi, pj := range p {
				if !intsEqual(pj, p2[pi]) {
					return false
				}
			}
			return true
		case []int64:
			for pi, pj := range p {
				if !intsEqual(pj, p2[pi]) {
					return false
				}
			}
			return true
		}
		return false

	case map[string]IntParams:
		if _, ok := i2.(map[string]IntParams); !ok {
			return false
		}

		for pk, pj := range p {
			if !intsEqual(pj, i2.(map[string]IntParams)[pk]) {
				return false
			}
		}
		return true

	case int64:
		if _, ok := i2.(int64); !ok {
			return false
		}

		return p == i2.(int64)

	default:
		return false
	}
}

/*func similar(p1 interface{}, p2 interface{}, delta float64) bool {
	return similar2(p1, p2, delta) && similar2(p2, p1, delta)
}

func similar2(p1 interface{}, p2 interface{}, delta float64) bool {
	if p1 == nil && p2 == nil {
		return true
	}
	switch p := p1.(type) {
	case []int64:
		switch p2 := p2.(type) {
		case []int64:
			for pi, pj := range p {
				if !similar2(pj, p2[pi], delta) {
					return false
				}
			}
			return true

		case []interface{}:
			for pi, pj := range p {
				if !similar2(pj, p2[pi], delta) {
					return false
				}
			}
			return true
		}
		return false

	case []float64:
		switch p2 := p2.(type) {
		case []float64:
			for pi, pj := range p {
				if !similar2(pj, p2[pi], delta) {
					return false
				}
			}
			return true

		case []interface{}:
			for pi, pj := range p {
				if !similar2(pj, p2[pi], delta) {
					return false
				}
			}
			return true
		}
		return false

	case []interface{}:
		switch p2 := p2.(type) {
		case []int64:
			for pi, pj := range p {
				if !similar2(pj, p2[pi], delta) {
					return false
				}
			}
			return true

		case []float64:
			for pi, pj := range p {
				if !similar2(pj, p2[pi], delta) {
					return false
				}
			}
			return true

		case []interface{}:
			for pi, pj := range p {
				if !similar2(pj, p2[pi], delta) {
					return false
				}
			}
			return true
		}
		return false

	case map[string]interface{}:
		if _, ok := p2.(map[string]interface{}); !ok {
			return false
		}

		for pk, pj := range p {
			if !similar(pj, p2.(map[string]interface{})[pk], delta) {
				return false
			}
		}
		return true

	case float64:
		if _, ok := p2.(float64); !ok {
			return false
		}

		return math.Abs(p-p2.(float64)) < delta

	case int64:
		if _, ok := p2.(int64); !ok {
			return false
		}

		return p == p2.(int64)

	default:
		return false
	}
}
*/

func saveAdd(a, b int64) int64 {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		logger.Fatal(SMPC, "", "INTEGER OVERFLOW IN SMPC ADDITION")
		return 0
	}
	return a + b
}

func arrayShuffle(arr []IntParams) {
	for i := len(arr) - 1; i > 0; i-- {
		nBig, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			logger.Fatal(SMPC, "", "Error in array shuffling %s", err)
		}
		n := nBig.Int64()
		arr[i], arr[n] = arr[n], arr[i]
	}
}

// TODO: alternative using the crypto rand package for multiplicative noise
// currently sometimes generates 0 shards that destroy all signal
// rs := float64(1.0)
// for i := 0; i < n-1; i++ {
// 	ri := big.NewInt(1)
// 	if rs > 1.0 {
// 		// we need to generate noise between 0 and 1
// 		// as we work with the integer representation, generation
// 		// noise between 0 and 10^exp will represent a float
// 		// between 0 and 1 in integer mode (see toInt64)
// 		var err error
// 		ri, err = rand.Int(rand.Reader, big.NewInt(int64(math.Pow10(exp))))
// 		if err != nil {
// 			logger.Fatal(SMPC, "", "Erorr in shard generation: %v", err)
// 		}
// 	} else {
// 		// we need to generate noise between 1 and maxNoise
// 		// The generated noise is at most 10^4 for the moment
// 		// as the noise is multiplicative that is more than enough
// 		var err error
// 		ri, err = rand.Int(rand.Reader, big.NewInt(int64(math.Pow10(4))))
// 		ri.Mul(ri, big.NewInt(int64(math.Pow10(exp))))
// 		fmt.Printf("PRODUCED RI > 1: %v\n", float64(ri.Int64())/math.Pow10(exp))
// 		if err != nil {
// 			logger.Fatal(SMPC, "", "Error in shard generation: %v", err)
// 		}
// 		if ri.Cmp(big.NewInt(int64(math.Pow10(4)))) == -1 {
// 			fmt.Printf("PRODUCED TOO SMALL SHARD: %v\n", ri)
// 		}
// 	}

// 	ps = append(ps, ri.Int64())
// 	// we keep the current product in the variable rs, however
// 	// we need to calculate in the float format for this
// 	rs *= float64(ri.Int64()) / math.Pow10(exp)
// 	fmt.Printf("Current rs is %v (%v)\n", rs, math.Log10(math.Abs(rs)))

// }
// // calculate last shard
// lastShard := toInt64(fromInt64(p, exp)/rs, exp)
// ps = append(ps, lastShard)
// arrayShuffle(ps)
