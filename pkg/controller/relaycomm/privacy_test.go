package relaycomm

import (
	"fc_controller/pkg/controller/enums"
	"fc_controller/pkg/controller/models"
	shared_link "fc_controller/pkg/shared/link"
	"testing"
)

func TestSMPCMergeParameters__RejectsMixedExponents(t *testing.T) {
	c := &RelayClient{}
	sameExponent := []models.SMPCMessageWrapper{
		{Data: int64(1), Operation: enums.SMPCOperationAdd, Exponent: 8},
		{Data: int64(2), Operation: enums.SMPCOperationAdd, Exponent: 8},
	}
	if err := c.SMPCMergeParameters(sameExponent); err != nil {
		t.Fatalf("equal exponents should be accepted: %v", err)
	}
	mixedExponent := []models.SMPCMessageWrapper{
		{Data: int64(1), Operation: enums.SMPCOperationAdd, Exponent: 8},
		{Data: int64(2), Operation: enums.SMPCOperationAdd, Exponent: 12},
	}
	if err := c.SMPCMergeParameters(mixedExponent); err == nil {
		t.Fatal("mixed exponents should be rejected")
	}

	byClient := map[shared_link.ClientID][]models.SMPCMessageWrapper{
		{1}: {mixedExponent[0]},
		{2}: {mixedExponent[1]},
	}
	if err := c.SMPCMergeParametersFirstAggregation(byClient); err == nil {
		t.Fatal("mixed exponents should be rejected in the first aggregation")
	}
	if err := c.SMPCMergeParametersSecondAggregation(byClient); err == nil {
		t.Fatal("mixed exponents should be rejected in the second aggregation")
	}
}
