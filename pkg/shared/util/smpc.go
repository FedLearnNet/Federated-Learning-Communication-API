package util

import (
	"crypto/rand"
	"encoding/binary"
	"fc_controller/pkg/shared/logger"
	"fmt"
	"math"
	"math/big"
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
		// Additive secret sharing over Z_{2^64}: n-1 shards are uniformly
		// random int64 values, the last one completes the sum. All int64
		// arithmetic wraps around modulo 2^64, so each shard on its own is
		// uniformly distributed and reveals nothing about p. Multiply inputs
		// are encoded by EncodeMultiplyInput beforehand, so they are shared
		// the same way.
		if operation != "add" && operation != "multiply" {
			logger.Fatal(SMPC, "", "Nonimplemented SMPC operation %s", operation)
			return nil
		}
		var rs int64
		for i := 0; i < n-1; i++ {
			r := randomInt64()
			rs += r // wraps around by design
			ps = append(ps, r)
		}
		ps = append(ps, p-rs)
		arrayShuffle(ps)

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
		// Shards and partial sums are summed modulo 2^64 for both operations
		// (multiply inputs are log-encoded). CheckSMPCAddRange guarantees
		// that the final sum of the true values fits into int64.
		var sum int64
		for _, pe := range pv {
			sum += pe // wraps around by design
		}
		s = sum

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

// randomInt64 returns a uniformly distributed int64 from crypto/rand
func randomInt64() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		logger.Fatal(SMPC, "", "Error in shard generation: %s", err)
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// randomNonZeroInt64 returns a uniformly distributed non-zero int64
func randomNonZeroInt64() int64 {
	for {
		if r := randomInt64(); r != 0 {
			return r
		}
	}
}

// maxSMPCSummand is the largest absolute value a single client may contribute
// so that the sum over all numClients clients still fits into int64
func maxSMPCSummand(numClients int) int64 {
	if numClients < 1 {
		numClients = 1
	}
	return math.MaxInt64 / int64(numClients)
}

// CheckSMPCAddRange ensures that the fixed point values in params can be
// summed over numClients clients without leaving the int64 range. As shards
// are aggregated modulo 2^64, this is what guarantees an exact result.
func CheckSMPCAddRange(params IntParams, numClients int) error {
	switch p := params.(type) {
	case []IntParams:
		for _, pe := range p {
			if err := CheckSMPCAddRange(pe, numClients); err != nil {
				return err
			}
		}
	case map[string]IntParams:
		for _, pe := range p {
			if err := CheckSMPCAddRange(pe, numClients); err != nil {
				return err
			}
		}
	case int64:
		bound := maxSMPCSummand(numClients)
		if p > bound || p < -bound {
			return fmt.Errorf("SMPC input %d (fixed point) exceeds the safe range of +-%d for %d clients, "+
				"use a smaller SMPC exponent or rescale the data", p, bound, numClients)
		}
	}
	return nil
}

// CheckFloatParams ensures that all numbers are finite and can be converted
// to fixed point numbers with the given exponent without overflowing int64
func CheckFloatParams(params interface{}, exp int) error {
	switch p := params.(type) {
	case []interface{}:
		for _, pe := range p {
			if err := CheckFloatParams(pe, exp); err != nil {
				return err
			}
		}
	case map[string]interface{}:
		for _, pe := range p {
			if err := CheckFloatParams(pe, exp); err != nil {
				return err
			}
		}
	case float64:
		if math.IsNaN(p) || math.IsInf(p, 0) {
			return fmt.Errorf("SMPC input %v is not a finite number", p)
		}
		if math.Abs(p*math.Pow10(exp)) >= math.MaxInt64 {
			return fmt.Errorf("SMPC input %v cannot be represented with exponent %d, "+
				"use a smaller SMPC exponent or rescale the data", p, exp)
		}
	}
	return nil
}

// EncodeMultiplyInput prepares floating point parameters for a secure
// multiplication. Each number v is replaced by three integers that are
// then summed with the additive SMPC:
//   - round(ln|v| * 10^exp): the sum gives the log of the product's magnitude
//   - isNegative(v) + 2*random: only the parity of the sum (the sign) is meaningful
//   - isZero(v) * random: the sum is non-zero iff any factor was zero
//
// Only the product itself can be decoded from the aggregate.
func EncodeMultiplyInput(params interface{}, exp int, numClients int) (IntParams, error) {
	switch p := params.(type) {
	case []interface{}:
		arr := []IntParams{}
		for _, pe := range p {
			enc, err := EncodeMultiplyInput(pe, exp, numClients)
			if err != nil {
				return nil, err
			}
			arr = append(arr, enc)
		}
		return arr, nil

	case map[string]interface{}:
		arr := map[string]IntParams{}
		for k, pe := range p {
			enc, err := EncodeMultiplyInput(pe, exp, numClients)
			if err != nil {
				return nil, err
			}
			arr[k] = enc
		}
		return arr, nil

	case float64:
		if math.IsNaN(p) || math.IsInf(p, 0) {
			return nil, fmt.Errorf("SMPC input %v is not a finite number", p)
		}
		var logMagnitude, sign, zero int64
		if p == 0 {
			zero = randomNonZeroInt64()
		} else {
			logFixed := math.Round(math.Log(math.Abs(p)) * math.Pow10(exp))
			if math.Abs(logFixed) > float64(maxSMPCSummand(numClients)) {
				return nil, fmt.Errorf("SMPC multiplication input %v cannot be represented with exponent %d "+
					"for %d clients, use a smaller SMPC exponent", p, exp, numClients)
			}
			logMagnitude = int64(logFixed)
		}
		if p < 0 {
			sign = 1
		}
		sign += 2 * randomInt64() // wraps around by design, the parity is kept
		return []IntParams{logMagnitude, sign, zero}, nil

	default:
		return p, nil
	}
}

// DecodeMultiplyResult converts the aggregated encoding produced by
// EncodeMultiplyInput back into the floating point product
func DecodeMultiplyResult(params IntParams, exp int) (interface{}, error) {
	switch p := params.(type) {
	case []IntParams:
		if len(p) == 3 {
			logMagnitude, ok1 := p[0].(int64)
			sign, ok2 := p[1].(int64)
			zero, ok3 := p[2].(int64)
			if ok1 && ok2 && ok3 {
				if zero != 0 {
					return float64(0), nil
				}
				magnitude := math.Exp(float64(logMagnitude) / math.Pow10(exp))
				if math.IsInf(magnitude, 0) {
					return nil, fmt.Errorf("the SMPC product exceeds the float64 range")
				}
				if sign&1 == 1 {
					return -magnitude, nil
				}
				return magnitude, nil
			}
		}
		arr := []interface{}{}
		for _, pe := range p {
			dec, err := DecodeMultiplyResult(pe, exp)
			if err != nil {
				return nil, err
			}
			arr = append(arr, dec)
		}
		return arr, nil

	case map[string]IntParams:
		arr := map[string]interface{}{}
		for k, pe := range p {
			dec, err := DecodeMultiplyResult(pe, exp)
			if err != nil {
				return nil, err
			}
			arr[k] = dec
		}
		return arr, nil

	default:
		return p, nil
	}
}

// PrepareSMPCInput validates the deserialized data sent by the app and
// converts it into the fixed point representation that is secret shared
// for the given SMPC operation
func PrepareSMPCInput(params interface{}, operation string, exp int, numClients int) (IntParams, error) {
	switch operation {
	case "add":
		if err := CheckFloatParams(params, exp); err != nil {
			return nil, err
		}
		intParams := FloatToInt(params, exp)
		if err := CheckSMPCAddRange(intParams, numClients); err != nil {
			return nil, err
		}
		return intParams, nil
	case "multiply":
		return EncodeMultiplyInput(params, exp, numClients)
	default:
		return nil, fmt.Errorf("unsupported SMPC operation %s", operation)
	}
}

// FinalizeSMPCResult converts the final SMPC aggregate into floating point numbers
func FinalizeSMPCResult(params IntParams, operation string, exp int) (interface{}, error) {
	if operation == "multiply" {
		return DecodeMultiplyResult(params, exp)
	}
	return IntToFloat(params, exp), nil
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
