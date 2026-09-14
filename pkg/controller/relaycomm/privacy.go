// Contains helper methods on the client class
// that connects the app and the global relay server related to SMPC operations
// As SMPC changes what is communicated how, this mostly takes care of the
// how it is communicated while the util/smpc package takes care of the actual SMPC logic
// of creating shards etc
package relaycomm

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/codec"
	"fc_controller/pkg/controller/enums"
	"fc_controller/pkg/controller/models"
	shared_link "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/logger"
	"fc_controller/pkg/shared/util"
	"fmt"
	"math/big"
)

// applies DP to the given data according to the given DP properties and returns the noised data
func (c *RelayClient) applyDp(outgoingMessage bridge.OutgoingMessage, deserializedData interface{}) (interface{}, error) {
	if !outgoingMessage.DpProps.Enabled {
		return deserializedData, nil
	}
	logger.Debug(LOCAL, "", "applyDp: noisetype=%q epsilon=%.5f delta=%.5e sensitivity=%v clippingVal=%v",
		outgoingMessage.DpProps.Noisetype, outgoingMessage.DpProps.Epsilon,
		outgoingMessage.DpProps.Delta, outgoingMessage.DpProps.Sensitivity, outgoingMessage.DpProps.ClippingVal)

	if deserializedData == nil {
		return nil, errors.New("DP operation requested but no data to apply it on")
	}

	noisedData, effectiveSensitivity, err := util.ApplyDP(
		deserializedData,
		outgoingMessage.DpProps.Noisetype,
		outgoingMessage.DpProps.Epsilon,
		outgoingMessage.DpProps.Delta,
		outgoingMessage.DpProps.ClippingVal,
		outgoingMessage.DpProps.Sensitivity)
	if err != nil {
		return nil, fmt.Errorf("error applying DP: %w", err)
	}

	logger.Info(LOCAL, "", "Applied DP with epsilon %.2f and delta %.2e to data with effective sensitivity %.2f", outgoingMessage.DpProps.Epsilon, outgoingMessage.DpProps.Delta, effectiveSensitivity)
	return noisedData, nil
}

// applySmpc generates one outgoingPacket per SMPC shard, one per recipient client.
func (c *RelayClient) applySmpc(outgoingMessage bridge.OutgoingMessage, deserializedData interface{}) ([]outgoingPacket, error) {
	shardsByClient, err := c.getShards(deserializedData, &outgoingMessage.SmpcProps)
	if err != nil {
		return nil, fmt.Errorf("failed to generate SMPC shards: %w", err)
	}

	smpcOperationByte, err := outgoingMessage.SmpcProps.Operation.ToByte()
	if err != nil {
		return nil, fmt.Errorf("failed to convert SMPC operation to byte: %w", err)
	}

	packets := make([]outgoingPacket, 0, len(shardsByClient))
	for clientID, shard := range shardsByClient {
		shardBytes, err := codec.Encode(shard, nil, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize shard for client %x: %w", clientID, err)
		}

		dest := clientID // copy loop variable before taking address
		packets = append(packets, outgoingPacket{
			cmd:                      byte(shared_link.CMD_SEND_SMPC_DATA),
			dest:                     &dest,
			currentSerializationMode: enums.DefaultSerializationMode,
			wireSerializationMode:    enums.DefaultSerializationMode,
			currentCompressionMode:   enums.DefaultCompressionMode,
			wireCompressionMode:      enums.DefaultCompressionMode,
			memoSize:                 outgoingMessage.MemoSize,
			memo:                     outgoingMessage.Memo,
			toAggregatorNameSize:     outgoingMessage.ToAggregatorNameSize,
			toAggregatorName:         outgoingMessage.ToAggregatorName,
			fromAggregatorNameSize:   outgoingMessage.FromAggregatorNameSize,
			fromAggregatorName:       outgoingMessage.FromAggregatorName,
			smpcExponent:             outgoingMessage.SmpcProps.Exponent,
			smpcOperation:            byte(smpcOperationByte),
			payload:                  shardBytes,
		})
	}

	return packets, nil
}

// getShards generates the recipient-to-shard mapping for one SMPC payload.
func (c *RelayClient) getShards(deserializedData interface{}, normalizedSmpcInfo *models.NormalizedSMPCProperties) (map[shared_link.ClientID]util.IntParams, error) {
	if deserializedData == nil {
		return nil, errors.New("SMPC operation requested but no data to apply it on")
	}

	clientIDs := make([]shared_link.ClientID, 0, len(c.runHandle.Meta.ClientId2PublicKey))
	for clientID := range c.runHandle.Meta.ClientId2PublicKey {
		clientIDs = append(clientIDs, clientID)
	}

	if normalizedSmpcInfo.NumShards <= 1 || normalizedSmpcInfo.NumShards > len(clientIDs) {
		return nil, errors.New("requested number of SMPC shards is not feasible")
	}

	shuffledClientIDs, err := cryptoShuffledClientIDs(clientIDs)
	if err != nil {
		return nil, err
	}

	intParamData := util.FloatToInt(deserializedData, int(normalizedSmpcInfo.Exponent))
	shards := util.Shard(intParamData, string(normalizedSmpcInfo.Operation), normalizedSmpcInfo.NumShards, int(normalizedSmpcInfo.Exponent))
	for len(shards) < len(shuffledClientIDs) {
		shards = append(shards, nil)
	}

	shardMap := make(map[shared_link.ClientID]util.IntParams, len(shuffledClientIDs))
	for i, clientID := range shuffledClientIDs {
		shardMap[clientID] = shards[i]
	}

	return shardMap, nil
}

// aggregateFromStore consumes one shard per client for a memo, rescales them to
// the maximum exponent, and aggregates the resulting shard set.
func (c *RelayClient) aggregateFromStore(store smpcShardStore, memo string) (util.IntParams, uint8, enums.SMPCOperation, error) {
	if store == nil || store[memo] == nil {
		return nil, 0, enums.SMPCOperationNone, errors.New("no SMPC shards available for memo")
	}
	if len(store[memo]) != c.runHandle.Meta.MaxNumberOfClients {
		return nil, 0, enums.SMPCOperationNone, errors.New("SMPC shards incomplete for aggregation")
	}

	maxExponent := uint8(0)
	operationUsed := enums.SMPCOperationNone
	hasOperation := false

	for _, element := range store[memo] {
		if len(element) == 0 {
			return nil, 0, enums.SMPCOperationNone, errors.New("empty SMPC shard queue entry")
		}

		if !hasOperation {
			operationUsed = element[0].Operation
			hasOperation = true
		} else if operationUsed != element[0].Operation {
			return nil, 0, enums.SMPCOperationNone, errors.New("different SMPC operations used by clients, cannot aggregate")
		}

		if element[0].Exponent > maxExponent {
			maxExponent = element[0].Exponent
		}
	}

	shards := make([]util.IntParams, 0, c.runHandle.Meta.MaxNumberOfClients)
	for key, element := range store[memo] {
		store[memo][key] = element[1:]
		shards = append(shards, util.Rescale(element[0].Data, element[0].Exponent, maxExponent))
	}

	aggParams := util.Aggregate(shards, string(operationUsed), int(maxExponent))
	return aggParams, maxExponent, operationUsed, nil
}

func (c *RelayClient) handleReceivedSMPCDataShard(fromClientID shared_link.ClientID, memo string,
	serializationModeUsedByte byte, compressionModeUsedByte byte,
	smpcExponentUsed byte, smpcOperationUsed enums.SMPCOperationByte, dataBytes []byte, header *shared_link.DataPacketHeader) error {
	logger.Info(LOCAL, "", "Received shard from Client %x", fromClientID)

	compressionModeUsed, err := enums.NormalizeCompressionModeByte(enums.CompressionModeByte(compressionModeUsedByte))
	if err != nil {
		return err
	}
	serializationModeUsed, err := enums.NormalizeSerializationModeByte(enums.SerializationModeByte(serializationModeUsedByte))
	if err != nil {
		return err
	}
	privKey := c.runHandle.Meta.OwnPrivateKey
	deserializedData, err := codec.Decode(dataBytes, &privKey, serializationModeUsed, compressionModeUsed)
	if err != nil {
		return err
	}
	smpcOperation, err := enums.NormalizeSMPCOperationByte(smpcOperationUsed)
	if err != nil {
		return err
	}
	params := models.SMPCMessageWrapper{
		Data:      util.NormalizeIntParams(deserializedData),
		Operation: smpcOperation,
		Exponent:  smpcExponentUsed,
	}

	c.smpcMu.Lock()
	defer c.smpcMu.Unlock()

	if _, exists := c.runHandle.Meta.ClientId2PublicKey[fromClientID]; !exists {
		return fmt.Errorf("received SMPC shard from unknown client %x", fromClientID)
	}

	if c.q[memo] == nil {
		c.q[memo] = make(map[shared_link.ClientID][]models.SMPCMessageWrapper)
	}
	if len(c.q[memo][fromClientID]) > 0 {
		clientIDAsUint64 := binary.BigEndian.Uint64(fromClientID[:])
		logger.Warn(LOCAL, "", "Received an SMPC shard for memo %s and clientID %016x (%064b) while there is already a message in the queue. Possible race condition!", memo, clientIDAsUint64, clientIDAsUint64)
	}
	c.q[memo][fromClientID] = append(c.q[memo][fromClientID], params)

	if len(c.q[memo]) < len(c.runHandle.Meta.ClientId2PublicKey) {
		return nil
	}

	// All shards received — merge exponents and aggregate
	if err := c.SMPCMergeParametersFirstAggregation(c.q[memo]); err != nil {
		return err
	}
	aggParams, maxExponent, smpcOpResult, err := c.aggregateFromStore(c.q, memo)
	if err != nil {
		return err
	}

	coordID := c.runHandle.Meta.CoordinatorID
	coordPubKey, ok := c.waitForPublicKey(coordID)
	if !ok {
		return fmt.Errorf("run became inactive while waiting for coordinator public key")
	}

	smpcOperationByte, err := smpcOpResult.ToByte()
	if err != nil {
		return err
	}

	encryptedAggBytes, err := codec.Encode(aggParams, &coordPubKey, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
	if err != nil {
		return err
	}

	memoStr := memo
	memoSize, memoBytes := memoToBinaryMemo(&memoStr)
	pkt := outgoingPacket{
		cmd:                    byte(shared_link.CMD_SEND_SMPC_AGG),
		dest:                   &coordID,
		wireSerializationMode:  enums.DefaultSerializationMode,
		wireCompressionMode:    enums.DefaultCompressionMode,
		memoSize:               memoSize,
		memo:                   memoBytes,
		toAggregatorNameSize:   header.ToAggregatorNameSize,
		toAggregatorName:       header.ToAggregatorNameBytes,
		fromAggregatorNameSize: header.FromAggregatorNameSize,
		fromAggregatorName:     header.FromAggregatorNameBytes,
		smpcExponent:           maxExponent,
		smpcOperation:          byte(smpcOperationByte),
		payload:                encryptedAggBytes,
	}
	return c.writePacketBytes(pkt)
}

func (c *RelayClient) handleReceivedSMPCAggregatedData(memo string, dataBytes []byte, serializationModeUsedByte byte, compressionModeUsedByte byte,
	fromClientID shared_link.ClientID, smpcExponentUsed byte, smpcOperationUsed enums.SMPCOperationByte, header *shared_link.DataPacketHeader) error {
	logger.Info(LOCAL, "", "Received aggregated shard from Client %x", fromClientID)

	smpcOperation, err := enums.NormalizeSMPCOperationByte(smpcOperationUsed)
	if err != nil {
		return err
	}
	compressionModeUsed, err := enums.NormalizeCompressionModeByte(enums.CompressionModeByte(compressionModeUsedByte))
	if err != nil {
		return err
	}
	serializationModeUsed, err := enums.NormalizeSerializationModeByte(enums.SerializationModeByte(serializationModeUsedByte))
	if err != nil {
		return err
	}
	privKey := c.runHandle.Meta.OwnPrivateKey
	deserializedData, err := codec.Decode(dataBytes, &privKey, serializationModeUsed, compressionModeUsed)
	if err != nil {
		return err
	}

	wrapper := models.SMPCMessageWrapper{
		Data:      util.NormalizeIntParams(deserializedData),
		Operation: smpcOperation,
		Exponent:  smpcExponentUsed,
	}

	c.smpcMu.Lock()
	defer c.smpcMu.Unlock()

	if c.qAggregator[memo] == nil {
		c.qAggregator[memo] = make(map[shared_link.ClientID][]models.SMPCMessageWrapper)
	}
	c.qAggregator[memo][fromClientID] = append(c.qAggregator[memo][fromClientID], wrapper)

	if len(c.qAggregator[memo]) < len(c.runHandle.Meta.ClientId2PublicKey) || !c.runHandle.Meta.IsCoordinator {
		return nil
	}

	// All aggregated shards received — final aggregation
	if err := c.SMPCMergeParametersSecondAggregation(c.qAggregator[memo]); err != nil {
		return err
	}
	aggParams, maxExponent, _, err := c.aggregateFromStore(c.qAggregator, memo)
	if err != nil {
		return err
	}

	return c.AddMessage(enums.SerializationGolangByte, enums.CompressionNoneByte, header, util.IntToFloat(aggParams, int(maxExponent)))
}

// SMPCMergeParametersFirstAggregation normalizes shards from different clients by rescaling them to
// a common exponent (the maximum across all clients) and ensures they use the same operation.
// This is called before the first aggregation step.
func (c *RelayClient) SMPCMergeParametersFirstAggregation(shardsByClient map[shared_link.ClientID][]models.SMPCMessageWrapper) error {
	if len(shardsByClient) == 0 {
		return errors.New("no shards to merge")
	}

	// Find the maximum exponent and ensure all use the same operation
	var maxExponent uint8
	var expectedOperation enums.SMPCOperation
	firstShard := true

	for clientID, shards := range shardsByClient {
		if len(shards) == 0 {
			return fmt.Errorf("client %x has no shards", clientID)
		}

		shardData := shards[0]
		if firstShard {
			maxExponent = shardData.Exponent
			expectedOperation = shardData.Operation
			firstShard = false
		} else {
			if shardData.Operation != expectedOperation {
				return fmt.Errorf("client %x used operation %v but expected %v", clientID, shardData.Operation, expectedOperation)
			}
			if shardData.Exponent > maxExponent {
				maxExponent = shardData.Exponent
			}
		}
	}

	// Rescale all shards to the maximum exponent
	for clientID, shards := range shardsByClient {
		for i, shard := range shards {
			if shard.Exponent != maxExponent {
				rescaledData := util.Rescale(shard.Data, shard.Exponent, maxExponent)
				shardsByClient[clientID][i].Data = rescaledData
				shardsByClient[clientID][i].Exponent = maxExponent
				logger.Info(LOCAL, "", "Rescaled shard from client %x from exponent %d to %d", clientID, shard.Exponent, maxExponent)
			}
		}
	}

	return nil
}

// SMPCMergeParametersSecondAggregation normalizes aggregated shards from different clients by rescaling
// them to a common exponent (the maximum across all clients) and ensures they use the same operation.
// This is called before the second (final) aggregation.
func (c *RelayClient) SMPCMergeParametersSecondAggregation(aggShardsByClient map[shared_link.ClientID][]models.SMPCMessageWrapper) error {
	if len(aggShardsByClient) == 0 {
		return errors.New("no aggregated shards to merge")
	}

	// Find the maximum exponent and ensure all use the same operation
	var maxExponent uint8
	var expectedOperation enums.SMPCOperation
	firstShard := true

	for clientID, shards := range aggShardsByClient {
		if len(shards) == 0 {
			return fmt.Errorf("client %x has no aggregated shards", clientID)
		}

		aggData := shards[0]
		if firstShard {
			maxExponent = aggData.Exponent
			expectedOperation = aggData.Operation
			firstShard = false
		} else {
			if aggData.Operation != expectedOperation {
				return fmt.Errorf("client %x sent aggregation with operation %v but expected %v", clientID, aggData.Operation, expectedOperation)
			}
			if aggData.Exponent > maxExponent {
				maxExponent = aggData.Exponent
			}
		}
	}

	// Rescale all aggregated shards to the maximum exponent
	for clientID, shards := range aggShardsByClient {
		for i, shard := range shards {
			if shard.Exponent != maxExponent {
				rescaledData := util.Rescale(shard.Data, shard.Exponent, maxExponent)
				aggShardsByClient[clientID][i].Data = rescaledData
				aggShardsByClient[clientID][i].Exponent = maxExponent
				logger.Info(LOCAL, "", "Rescaled aggregated shard from client %x from exponent %d to %d", clientID, shard.Exponent, maxExponent)
			}
		}
	}

	return nil
}

// SMPCMergeParameters is a generic helper that normalizes SMPC parameters by rescaling them to
// a common exponent (the maximum across all wrappers) while ensuring consistent operations.
func (c *RelayClient) SMPCMergeParameters(wrappers []models.SMPCMessageWrapper) error {
	if len(wrappers) == 0 {
		return errors.New("no SMPC parameters to merge")
	}

	// Find max exponent and ensure consistent operation
	maxExponent := wrappers[0].Exponent
	expectedOperation := wrappers[0].Operation

	for i, wrapper := range wrappers {
		if wrapper.Operation != expectedOperation {
			return fmt.Errorf("wrapper %d has operation %v but expected %v", i, wrapper.Operation, expectedOperation)
		}
		if wrapper.Exponent > maxExponent {
			maxExponent = wrapper.Exponent
		}
	}

	// Rescale all to max exponent
	for i := range wrappers {
		if wrappers[i].Exponent != maxExponent {
			rescaledData := util.Rescale(wrappers[i].Data, wrappers[i].Exponent, maxExponent)
			wrappers[i].Data = rescaledData
			wrappers[i].Exponent = maxExponent
		}
	}

	return nil
}

// cryptoShuffledClientIDs returns a crypto-random permutation of recipient client IDs.
func cryptoShuffledClientIDs(clientIDs []shared_link.ClientID) ([]shared_link.ClientID, error) {
	shuffled := make([]shared_link.ClientID, len(clientIDs))
	copy(shuffled, clientIDs)

	for i := len(shuffled) - 1; i > 0; i-- {
		jBig, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return nil, err
		}
		j := int(jBig.Int64())
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled, nil
}
