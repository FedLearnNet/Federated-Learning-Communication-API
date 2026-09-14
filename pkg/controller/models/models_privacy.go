// Contains all models related to privacy enhancing techniques (PETs) such as SMPC, DP
package models

import (
	enums "fc_controller/pkg/controller/enums"
	"fmt"

	"fc_controller/pkg/shared/logger"
	util "fc_controller/pkg/shared/util"
)

const PETLOGSTRING = "PRIVACY_ENHANCING_TECHNIQUES_SETTINGS"

// SMPC
type SMPCProperties struct {
	Operation string `json:"operation"` // "add" or "multiply"
	Exponent  *uint8 `json:"exponent"`
	NumShards *int   `json:"numShards"`
	Enabled   bool   `json:"enabled"`
}

type NormalizedSMPCProperties struct {
	Operation enums.SMPCOperation
	Exponent  uint8
	NumShards int
	Enabled   bool
}

// SMPCMessageWrapper stores one SMPC message payload plus metadata needed
// to validate aggregation compatibility across clients.
type SMPCMessageWrapper struct {
	Data      util.IntParams      `json:"data"`
	Operation enums.SMPCOperation `json:"operation"`
	Exponent  uint8               `json:"exponent"`
}

// DP
type DPProperties struct {
	Noisetype   *string  `json:"noisetype"` // "laplace" or "gauss"
	Epsilon     *float64 `json:"epsilon"`
	Delta       *float64 `json:"delta"`
	Sensitivity *float64 `json:"sensitivity,omitempty"`
	ClippingVal *float64 `json:"clippingVal,omitempty"`
	Enabled     bool     `json:"enabled"`
}

type NormalizedDPProperties struct {
	Enabled     bool
	Noisetype   string
	Epsilon     float64
	Delta       float64
	Sensitivity *float64
	ClippingVal *float64
}

// NormalizeDpProperties sets all variables concerning differential Privacy (DP), also performs
// some checks on the variables and sets default values if necessary.
// Must be called with a non nil dpinfo and returns an error in case of invalid settings
func (dpinfo *DPProperties) NormalizeDpProperties() (NormalizedDPProperties, error) {
	if dpinfo == nil {
		return NormalizedDPProperties{}, fmt.Errorf("DP properties are nil")
	}

	normalized := NormalizedDPProperties{
		Enabled:     true,
		Noisetype:   "laplace",
		Epsilon:     0.99999,
		Delta:       0,
		ClippingVal: dpinfo.ClippingVal,
		Sensitivity: dpinfo.Sensitivity,
	}

	// Noise type: default laplace, only allow laplace and gauss
	if dpinfo.Noisetype == nil {
		logger.Warn(PETLOGSTRING, "",
			"DP: No noise type given, using laplace noise")
	} else if *(dpinfo.Noisetype) != "laplace" && *(dpinfo.Noisetype) != "gauss" {
		logger.Warn(PETLOGSTRING, "",
			"DP: Invalid noise type given, using laplace noise")
	} else {
		normalized.Noisetype = *(dpinfo.Noisetype)
	}

	// Epsilon and Delta
	if dpinfo.Epsilon == nil {
		logger.Warn(PETLOGSTRING, "",
			"DP: No epsilon given, using epsilon = 0.9999")
	} else {
		normalized.Epsilon = *dpinfo.Epsilon
	}
	if dpinfo.Delta == nil {
		if normalized.Noisetype == "laplace" {
			// laplace does not allow delta != 0
			normalized.Delta = 0.0
		} else {
			normalized.Delta = 0.01
		}
		logger.Warn(PETLOGSTRING, "",
			"DP: No delta given, using delta = %f", normalized.Delta)
	}

	// make sure ClippingVal and Sensitivity are non negative
	if normalized.Sensitivity != nil {
		if *(normalized.Sensitivity) <= 0.0 {
			logger.Error(PETLOGSTRING, "", "Sensitivity given is "+
				"negative or 0 and cannot be applied")
			return NormalizedDPProperties{}, fmt.Errorf("Negative/0 sensitivity given")
		}
	}
	if normalized.ClippingVal != nil {
		if *(normalized.ClippingVal) < 0.0 {
			logger.Error(PETLOGSTRING, "", "ClippingVal given is "+
				"negative and cannot be applied")
			return NormalizedDPProperties{}, fmt.Errorf("clippingVal given is negative")
		} else if *(normalized.ClippingVal) == 0.0 {
			// gets treated as no clipping val, e.g. test-app gives
			// 0 instead of nil
			normalized.ClippingVal = nil
		}
	}

	// Either clippingVal or sensitivity must be given
	// Default to using clippingVal of 10
	if normalized.Sensitivity == nil &&
		normalized.ClippingVal == nil {
		// If neither value are given, use a default
		// clippingval as sensitivity is data dependant
		normalized.ClippingVal = new(float64)
		*normalized.ClippingVal = 10.0
		logger.Warn(PETLOGSTRING, "",
			"DP: Neither Sensitivity nor a ClippingVal given, "+
				"clipping with ClippingVal = 10.0")
	}

	// make sure that epsilon and delta are choosen correctly
	// for laplace and gauss noise
	// gauss
	if normalized.Noisetype == "gauss" {
		// epsilon \in (0,1)
		if normalized.Epsilon >= 1.0 {
			normalized.Epsilon = 0.99999
			logger.Warn(PETLOGSTRING, "",
				"Invalid epsilon given, for "+
					"gauss noise, epsilon \\in (0,1) is needed, given "+
					"value was too high and corrected to 0.99999")
		}
		if normalized.Epsilon <= 0.0 {
			normalized.Epsilon = 0.00001
			logger.Warn(PETLOGSTRING, "", "Invalid epsilon given, for "+
				"gauss noise, epsilon \\in (0,1) is needed, given "+
				"value was too low and corrected to 0.00001")
		}
		if normalized.Delta <= 0.0 {
			// Delta must be != 0 and positive
			normalized.Delta = 0.00001
			logger.Warn(PETLOGSTRING, "", "Invalid delta given, for "+
				"gauss noise, delta >= 0 is needed, given "+
				"value was too low and corrected to 0.00001")
		}
		if normalized.Delta >= 1.0 {
			normalized.Delta = 0.99999
			logger.Warn(PETLOGSTRING, "", "Delta >= 1.0 does not "+
				"make sense as delta is a probability. Delta "+
				"was cahnged to 0.99999, which is still very "+
				"dangerously high")
		}
	}
	//laplace
	if normalized.Noisetype == "laplace" {
		// epsilon must be positive
		if normalized.Epsilon <= 0.0 {
			normalized.Epsilon = 0.00001
			logger.Warn(PETLOGSTRING, "", "Invalid epsilon given, "+
				"epsilon must be > 0, "+
				"value was too low and corrected to 0.00001")
		}
		if normalized.Delta != 0.0 {
			// Delta must be != 0 and positive
			normalized.Delta = 0.0
			logger.Warn(PETLOGSTRING, "", "Invalid delta given, for "+
				"laplace noise, delta must be 0 and was "+
				"changed to 0")
		}
	}

	return normalized, nil
}

// NormalizeSmpcProperties returns the normalizated version of
// all variables concerning secure multi-party computation (SMPC),
// also performs some checks on the variables and uses default values if necessary.
// Must be called with a non nil smpcinfo and returns an error in case of invalid settings
func (smpcinfo *SMPCProperties) NormalizeSmpcProperties(numClients int) (NormalizedSMPCProperties, error) {
	if smpcinfo == nil {
		return NormalizedSMPCProperties{}, fmt.Errorf("SMPC properties are nil")
	}
	normalizedOperation, err := enums.NormalizeSMPCOperation(smpcinfo.Operation)
	if err != nil {
		return NormalizedSMPCProperties{}, err
	}
	// Default values
	normalized := NormalizedSMPCProperties{
		Enabled:   true,
		NumShards: numClients,
		Exponent:  uint8(8),
		Operation: normalizedOperation,
	}

	// Use given shards if valid (between 1 and numClients), otherwise use default
	if smpcinfo.NumShards != nil &&
		(*smpcinfo.NumShards) > 0 &&
		(*smpcinfo.NumShards) <= numClients {
		normalized.NumShards = *smpcinfo.NumShards
	}
	// Use given Exponent if valid (between 2 and 16), otherwise use default
	if smpcinfo.Exponent != nil {
		// Later we generate a randomnumber of size 53 bits
		// When the exponent is 16, this needs approximately 53.152 bits
		// to be represented, leaving the noise at a similar scale.
		// Any bigger exponent cannot be supported well as the noise
		// would be to low to mask the original value well.
		if *smpcinfo.Exponent < 2 {
			logger.Warn(PETLOGSTRING, "", "Exponent given is too low for meaningful noise generation, we suggest using an exponent bigger than 1")
		} else if *smpcinfo.Exponent > 16 {
			logger.Warn(PETLOGSTRING, "", "Exponent given is quite high, we suggest using an exponent smaller than 16 or the generated noise becomes insignificant in comparison to the given values")
		} else {
			normalized.Exponent = *smpcinfo.Exponent
		}
	}

	return normalized, nil
}
