// Package has 4 main functions:
// ApplyDP
//
//	clipps values using ClipValuesGlobal if clippingValPtr != nil, then adds noise to the
//	deserialized data object provided by the caller
//
// GetWeightsNormTotal
//
//	calculates the p-norm of given data
//
// ClipValuesGlobal
//
//	clips data so the given p-norm of the data is clippingVal
//
// AddNoise
//
//	adds noise to given data so the data gets differential private
//
// see function descriptions for more information
package util

import (
	"errors"
	"fmt"
	"math"

	noise "github.com/google/differential-privacy/go/noise"
)

// Main Function of the package
// given dataDP, a deserialized data object, differential privacy gets applied
// according to the further variables. Serialization and deserialization are handled by the caller.
// if clippingValPtr is not nil, its value gets used in the following manner:
// in case of noisetype == laplace:
//
//	the L1-norm of all numeric Data combined in dataBytes is clipped to *clippingValPtr
//
// in case of noisetype == gauss:
//
//	the L2-norm of all numeric Data combined in dataBytes is clipped to *clippingValPtr
//
// other noisetypes are not supported
// Either clippingValPtr and/or sensitivityPtr must be set.
// If neither is provided, cannot determine how much noise to add and an error is returned.
// Input:
//
//	dataDP             		- Contains the deserialized data object to which DP is applied
//	noisetype string       	- either "gauss" or "laplace", determines which distribution the noise
//	                         will be drawn from
//	epsilon, delta float64  - variables to determine the amount of noise and privacy to be added
//	                         see e.g. Cynthia Dwork and Aaron Roth. “The Algorithmic Foundations of
//	                         Differential Privacy. for more information
//	clippingValPtr *float64 - if != nil, points to the clippingValue. Clipping is explained in
//	                         ClipValuesGlobal
//	sensitivityPtr *float64 - optional sensitivity; if nil, clipping-derived sensitivity is used
//
// Output:
//
//	data interface{}         - same as Input dataDP, but with added noise
//	effectiveSensitivityPtr  - effective sensitivity used for noise application
//	err error                - an Error object or nil if no error ocurred
func ApplyDP(dataDP interface{},
	noisetype string,
	epsilon, delta float64,
	clippingValPtr *float64,
	sensitivityPtr *float64) (interface{}, float64, error) {
	effectiveSensitivityPtr := sensitivityPtr
	var effectiveSensitivity float64

	// Check if all given values are valid for DP
	err := checkDPArgs(noisetype,
		epsilon, delta,
		effectiveSensitivityPtr, clippingValPtr)
	if err != nil {
		return dataDP, 0, err
	}

	// apply clipping if necessary
	if clippingValPtr != nil {
		var sensClipping float64
		var err error = nil
		dataDP, sensClipping, err = ClipValuesGlobal(
			dataDP, noisetype, *(clippingValPtr))
		if err != nil {
			return dataDP, 0, fmt.Errorf("Error while clipping: %s", err.Error())
		}
		// In case clipping gave a smaller
		// Sensitivity (sensClipping) than
		// the given Sensitivity, use sensClipping for
		// sensitivityPtr
		if effectiveSensitivityPtr == nil ||
			*effectiveSensitivityPtr > sensClipping {
			effectiveSensitivityPtr = &sensClipping
		}
	}

	if effectiveSensitivityPtr == nil {
		return dataDP, 0, errors.New("sensitivity is nil; provide sensitivity or clipping value")
	}
	effectiveSensitivity = *effectiveSensitivityPtr
	// add noise
	dataDP, err = AddNoise(dataDP, effectiveSensitivity,
		epsilon, delta,
		noisetype)
	if err != nil {
		return dataDP, 0, err
	}

	return dataDP, effectiveSensitivity, nil
}

// Check arguments given to DP functions for validity, returns an error if any value is invalid
func checkDPArgs(noisetype string,
	epsilon, delta float64,
	sensitivityPtr, clippingValPtr *float64) error {
	// Delta must be 0 for laplace and epsilon > 0
	if noisetype == "laplace" {
		if delta != 0.0 {
			return errors.New("Laplace noise does not " +
				"support delta != 0.0")
		}
	}

	// Delta must be NOT 0 for gauss
	if noisetype == "gauss" {
		if delta <= 0.0 {
			return errors.New("Gauss noise does not " +
				"support delta = 0.0 or negative delta")
		}
	}

	// epsilon, clippingval, Sensitivity must be
	// non negative
	// Epsilon must be > 0 (division by epsilon later)
	if epsilon <= 0.0 {
		return errors.New("Epsilon given " +
			"is negative/0, " +
			"must be > 0")
	}
	if sensitivityPtr != nil {
		if *(sensitivityPtr) < 0.0 {
			return errors.New("Sensitivity given " +
				"is negative, " +
				"must be >= 0")
		}
	}

	if clippingValPtr != nil {
		if *(clippingValPtr) < 0.0 {
			return errors.New("ClippingVal given " +
				"is negative, " +
				"must be >= 0")
		}
	}

	// No errors
	return nil

}

////// Clipping Value //////
// func getWeightsNormSumTotalInternal -> calculates the sum part of the norm
// func GetWeightsNormTotal -> uses getWeightsNormSumTotalInternal and applies
//                             math.Sqrt if needed to get the actual norm
// func clipInternal -> Does the clipping
// func ClipValuesGlobal -> clips values given to it

// Iterates over the whole of data, including any subarray and subsubarray, etc.
// Calculates the sum part of ||all numeric data||normtype norm.
// e.g. Input is array [1,2,3], normtype = 2,
// calculates 1^2 + 2^2 + 3^2
// Currently only supports the 1-norm and 2-norm
func getWeightsNormSumTotalInternal(data interface{}, normtype int8) float64 {
	var curWeightnorm float64 = 0.0
	switch p := data.(type) {

	// An multielement Datatype
	case []interface{}:
		for _, dataElement := range p {
			// dataElement might be any datatype, so recursively call this
			// function and add up the curWeightnorm
			curWeightnorm += getWeightsNormSumTotalInternal(dataElement, normtype)
		}
		return curWeightnorm

	// a hashtable containing multple elements per key
	case map[string][]interface{}:
		for _, dataElement := range p {
			curWeightnorm += getWeightsNormSumTotalInternal(dataElement, normtype)
		}
		return curWeightnorm

	// a hashtable containing a single element
	case map[string]interface{}:
		for _, dataElement := range p {
			curWeightnorm += getWeightsNormSumTotalInternal(dataElement, normtype)
		}
		return curWeightnorm

	// primitive cases, just square/abs
	case float64:
		if normtype == 1 {
			return math.Abs(p)
		} else if normtype == 2 {
			return math.Pow(p, 2.0)
		}

	case float32:
		if normtype == 1 {
			return math.Abs(float64(p))
		} else if normtype == 2 {
			return math.Pow(float64(p), 2.0)
		}

	// primitive cases, just square/abs
	// has to be converted to float
	case int64:
		if normtype == 1 {
			if p < 0 {
				p = -p
			}
			return float64(p)
		} else if normtype == 2 {
			x := p * p
			if p != 0 && x/p != p {
				// overflow handling
				return math.Inf(1)
			}
			return float64(x)
		}
	case int:
		if normtype == 1 {
			if p < 0 {
				p = -p
			}
			return float64(p)
		} else if normtype == 2 {
			pInt64 := int64(p)
			x := pInt64 * pInt64
			if pInt64 != 0 && x/pInt64 != pInt64 {
				// overflow handling
				return math.Inf(1)
			}
			return float64(x)
		}
		// any other case gets handled outside the switch statement by returning curWeightnorm = 0.0
		// e.g. in case of a string just return 0.0
	}
	return curWeightnorm
}

// Calculates the p-norm of all numerical elements of data
// currently only supports 1-norm and 2-norm
// e.g. strings or other non numeric types get ignored
// Input:
//
//	data            - any object, only the numerical values will be evaluated
//	                  only supports float64, float32, int64 and int
//	normtype int8   - only 1 or 2 allowed, normtype = p in p-norm
//
// Output:
//
//	clips the values in data and returns:
//	 weightsNorm - The calculated norm
//	 err         - nil if no error, else an Error object
func GetWeightsNormTotal(data interface{}, normtype int8) (float64, error) {

	if normtype != 1 && normtype != 2 {
		return -1.0, errors.New("Invalid normtype, only norm 1 and 2 are supported")
	}

	sum := getWeightsNormSumTotalInternal(data, normtype)
	if sum == math.Inf(1) {
		return sum, errors.New("Overflow, could not calculate the norm of the given data")
	}
	if normtype == 2 {
		return math.Sqrt(sum), nil
	} else if normtype == 1 {
		return sum, nil
	}
	// should not happen, normtype neither 1 nor 2 and
	//normtype being 1 or 2 return values
	return -1.0, errors.New("Unknown Error")
}

// clips given data according to the factorClipping, which should be < 1.0
// for outside use, use the function ClipValuesGlobal, this is just a helper function
func clipInternal(data interface{},
	factorClipping float64) interface{} {
	// clips the given data for ClipValuesGlobal, manages slices
	switch p := data.(type) {
	// An multielement Datatype
	case []interface{}:
		var clippedData []interface{}
		for range p {
			// append by empty interface slices to then fill them
			clippedData = append(clippedData, []interface{}{})
		}
		// fill up the created empty interface array
		for idx, val := range p {
			// data is a slice, so get clippedData for each element in
			// the current slice
			curElementClipped := clipInternal(val,
				factorClipping)
			clippedData[idx] = curElementClipped
		}
		return clippedData

	// a hashtable containing multple elements per key
	case map[string][]interface{}:
		var clippedData = make(map[string]interface{})
		// fill up the created empty interface map
		for key, val := range p {
			// data is a map to slices, so get clippedData for each
			// key/val pair
			curElementClipped := clipInternal(val, factorClipping)
			clippedData[key] = curElementClipped
		}
		return clippedData

	// a hashtable containing a single element
	case map[string]interface{}:
		clippedData := make(map[string]interface{})
		// fill up the created empty interface array
		for key, val := range p {
			// data is a map to some interface, so get clippedData for each
			// key/val pair
			curElementClipped := clipInternal(val, factorClipping)
			clippedData[key] = curElementClipped
		}
		return clippedData

	//primitive cases
	case float64:
		return p * factorClipping
	case float32:
		return float64(p) * factorClipping
	case int64:
		return float64(p) * factorClipping
	case int:
		return float64(p) * factorClipping

	// default case, for any other type, e.g. a string
	// just copy the value in clippedData and ignore it
	default:
		return data
	}
}

// clips all values in data according to the clippingVal
// formula used is:
// for any element x in data:
//
//	x * 1/max(1, (||data||normtype) / (clippingVal) ),
//
// where normtype is 1 for laplace noise and 2 for gauss noise
// in practive, check if clippingVal / dataNorm < 1
// if yes, clip any element x:
//
//	x =  x * (clippingVal/weightsNorm)
//
// if not, nothing has to be done
// Sensitivity is 2 * clippingVal, since when clipped, for any two outputs, the biggest difference
// would be for one outputs norm being +clippingVal and for the other to be -clippingVal
// Input:
//
//	data            - any object, only the numerical values will be evaluated
//	                  only supports float64, float32, int64 and int
//	noisetype string- either "gauss" or "laplace", determines which distribution the noise
//	                  will be drawn from
//	clippingVal     - the data will be scaled down so the 1-norm(laplace) / 2-norm(gauss)
//	                  of all numerical data will be clippingVal
//
// Output:
//
//	clips the values in data and returns:
//	 sensitivity - float64 of clippingVal * 2
//	 err         - nil if no error, else an Error object
func ClipValuesGlobal(data interface{}, noisetype string, clippingVal float64) (
	interface{}, float64, error) {
	var normtype int8
	if noisetype == "laplace" {
		normtype = 1
	} else if noisetype == "gauss" {
		normtype = 2
	} else {
		return data, 0.0, errors.New("Only laplace and " +
			"gauss noise are supported")
	}

	weightsNorm, err := GetWeightsNormTotal(data, normtype)
	if err != nil {
		return data, 0.0, err
	}

	// Check if clipping is necessary
	factorClipping := clippingVal / weightsNorm
	var clippedData interface{}
	if factorClipping < 1 {
		// In this case clip, otherwise the data is sufficiently private
		switch data.(type) {
		// This ensures that in case data is just one number, the resulting
		// data is not a slice containing said clipped number
		case float64, float32, int64, int:
			clippedData = clipInternal(data, factorClipping)
		default:
			clippedData = clipInternal(data, factorClipping)
		}
	} else {
		clippedData = data
	}
	sensitivity := 2 * clippingVal

	return clippedData, sensitivity, nil
}

////// Add Noise //////
// func addNoiseInternal -> actually adds noise to an given
//                               interface{} object.
// func AddNoise -> adds noise according to given parameters
//                  applies the standard dp lapalce/gauss mechanism

// Helper function, applies noise to data according to given variables
// use AddNoise function instead, this is just an internal function
func addNoiseInternal(data interface{}, sensitivity float64,
	epsilon float64, delta float64,
	noisetype string) (interface{}, error) {
	// Adds noise to data, where data is some kind of multielement object, manages slices
	switch p := data.(type) {
	// An multielement Datatype
	case []interface{}:
		var noisedData []interface{}
		for range p {
			// append by empty interface slices to then fill them
			noisedData = append(noisedData, []interface{}{})
		}
		// fill up the created empty interface array
		for idx, val := range p {
			// data is a slice, so get noisedData for each element in
			// the current slice
			// switch case so in case one element in the slice is
			// just a number, it gets returned as a number and not
			// as a slice of size 1
			curElementNoised, err := addNoiseInternal(
				val, sensitivity, epsilon, delta, noisetype)
			if err != nil {
				return data, err
			}
			noisedData[idx] = curElementNoised
		}
		return noisedData, nil

	// a hashtable containing multple elements per key
	case map[string][]interface{}:
		noisedData := make(map[string]interface{})
		// fill up the created empty interface map
		for key, val := range p {
			// data is a map to slices, so get clippedData for each
			// key/val pair
			curElementNoised, err := addNoiseInternal(
				val, sensitivity, epsilon, delta, noisetype)
			if err != nil {
				return data, err
			}
			noisedData[key] = curElementNoised
		}
		return noisedData, nil

	// a hashtable containing any one or multiple elements
	case map[string]interface{}:
		noisedData := make(map[string]interface{})
		// fill up the created empty interface array
		for key, val := range p {
			// data is a map to some interface, so get clippedData for each
			// key/val pair
			curElementNoised, err := addNoiseInternal(
				val, sensitivity, epsilon, delta, noisetype)
			if err != nil {
				return data, err
			}
			noisedData[key] = curElementNoised
		}
		return noisedData, nil

		//primitive cases
	case float64:
		if noisetype == "laplace" {
			lpNoise := noise.Laplace()
			return lpNoise.AddNoiseFloat64(p, 1, sensitivity,
				epsilon, delta), nil
		} else if noisetype == "gauss" {
			gaussNoise := noise.Gaussian()
			return gaussNoise.AddNoiseFloat64(p, 1, sensitivity,
				epsilon, delta), nil
			// AddNoiseFloat64(x float64, l0Sensitivity int64,
			// lInfSensitivity, epsilon, delta float64) (float64, error)
			// l2Sensitivity := lInfSensitivity * math.Sqrt(float64(l0Sensitivity))
			// use 1 for l0 Sensitivity -> l2Sensitivity = lInfSensitivity * 1
			// so give l2 sensitivity as lInfSensitivity
		} else {
			return data, errors.New("Invalid noisetype given")
		}
	case float32:
		if noisetype == "laplace" {
			lpNoise := noise.Laplace()
			return lpNoise.AddNoiseFloat64(float64(p), 1, sensitivity,
				epsilon, delta), nil
		} else if noisetype == "gauss" {
			gaussNoise := noise.Gaussian()
			return gaussNoise.AddNoiseFloat64(float64(p), 1, sensitivity,
				epsilon, delta), nil
			// AddNoiseFloat64(x float64, l0Sensitivity int64,
			// lInfSensitivity, epsilon, delta float64) (float64, error)
			// l2Sensitivity := lInfSensitivity * math.Sqrt(float64(l0Sensitivity))
			// use 1 for l0 Sensitivity -> l2Sensitivity = lInfSensitivity * 1
			// so give l2 sensitivity as lInfSensitivity
		} else {
			return data, errors.New("Invalid noisetype given")
		}
	case int64:
		// there is an AddNoiseInt64 function, however, it only
		// takes an int64 for Sensitivity, therefore, we use AddNoiseFloat64
		if noisetype == "laplace" {
			lpNoise := noise.Laplace()
			return lpNoise.AddNoiseFloat64(float64(p), 1, sensitivity,
				epsilon, delta), nil
		} else if noisetype == "gauss" {
			gaussNoise := noise.Gaussian()
			return gaussNoise.AddNoiseFloat64(float64(p), 1, sensitivity,
				epsilon, delta), nil
			// AddNoiseFloat64(x float64, l0Sensitivity int64,
			// lInfSensitivity, epsilon, delta float64) (float64, error)
			// l2Sensitivity := lInfSensitivity * math.Sqrt(float64(l0Sensitivity))
			// use 1 for l0 Sensitivity -> l2Sensitivity = lInfSensitivity * 1
			// so give l2 sensitivity as lInfSensitivity
		} else {
			return data, errors.New("Invalid noisetype given")
		}
	case int:
		// there is an AddNoiseInt64 function, however, it only
		// takes an int64 for Sensitivity, therefore, we use AddNoiseFloat64
		if noisetype == "laplace" {
			lpNoise := noise.Laplace()
			return lpNoise.AddNoiseFloat64(float64(p), 1, sensitivity,
				epsilon, delta), nil
		} else if noisetype == "gauss" {
			gaussNoise := noise.Gaussian()
			return gaussNoise.AddNoiseFloat64(float64(p), 1, sensitivity,
				epsilon, delta), nil
			// AddNoiseFloat64(x float64, l0Sensitivity int64,
			// lInfSensitivity, epsilon, delta float64) (float64, error)
			// l2Sensitivity := lInfSensitivity * math.Sqrt(float64(l0Sensitivity))
			// use 1 for l0 Sensitivity -> l2Sensitivity = lInfSensitivity * 1
			// so give l2 sensitivity as lInfSensitivity
		} else {
			return data, errors.New("Invalid noisetype given")
		}

	// default case, for any other type, e.g. a string
	// just copy the value in noisedData and ignore it
	default:
		return data, nil
	}
}

// adds noise to data according to the given variables
// Input:
//
//	data                    - any object, only the numerical values will be evaluated
//	                          only supports float64, float32, int64 and int
//	sensitivity float64     - the sensitivity to be used for the noise distribution, the higher,
//	                          the more noise gets added
//	epsilon, delta float64   - variables to determine the amount of noise and privacy to be added
//	                          see e.g. Cynthia Dwork and Aaron Roth. “The Algorithmic Foundations of
//	                          Differential Privacy. for more information
//	noisetype string        - either "gauss" or "laplace", determines which distribution the noise
//	                          will be drawn from
//
// Output:
//
//	data interface{} - same as input data, but with added noise
//	err error        - an Error object or nil if no error ocurred
func AddNoise(data interface{}, sensitivity float64,
	epsilon float64, delta float64,
	noisetype string) (interface{}, error) {
	switch data.(type) {
	// This ensures that in case data is just one number, the resulting
	// data is not a slice containing said clipped number
	case float64, float32, int64, int:
		data, err := addNoiseInternal(data, sensitivity,
			epsilon, delta,
			noisetype)
		return data, err
	default:
		data, err := addNoiseInternal(data, sensitivity,
			epsilon, delta,
			noisetype)
		return data, err
	}
}
