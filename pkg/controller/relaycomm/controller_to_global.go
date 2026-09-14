// app --> [CONTROLLER] --> global relay server
// Contains all code related to handling messages
// the local app handler offloaded here
// processes the message and
// relays it to the global relay server
package relaycomm

import (
	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/codec"
	"fc_controller/pkg/controller/enums"
	shared_link "fc_controller/pkg/shared/link"
	logger "fc_controller/pkg/shared/logger"
	shared_util "fc_controller/pkg/shared/util"
	"sync"
	"time"
)

// outgoingPacket holds fully-encoded bytes and header metadata for one wire packet.
// payload is always unencrypted here; the send goroutine handles encryption before writing.
type outgoingPacket struct {
	cmd                      byte
	dest                     *shared_link.ClientID
	currentSerializationMode enums.SerializationMode
	wireSerializationMode    enums.SerializationMode
	currentCompressionMode   enums.CompressionMode
	wireCompressionMode      enums.CompressionMode
	memoSize                 byte
	memo                     []byte
	fromAggregatorNameSize   byte
	fromAggregatorName       []byte
	toAggregatorNameSize     byte
	toAggregatorName         []byte
	smpcExponent             byte
	smpcOperation            byte
	// Should be serialized and compressed according to currentSerializationMode and currentCompressionMode
	payload []byte
}

func (c *RelayClient) watchOutgoing() {
	for range c.runHandle.OutgoingNotify {
		logger.Debug(RELAYCLIENT, "", "Received outgoing notify, checking for messages to relay")
		for {
			// SafeDequeue checks if run is active and blocks cleanup (MarkInactive)
			// from proceeding until this dequeue completes.
			outgoingMessage, ok := c.runHandle.SafeDequeue()
			if !ok {
				// Queue empty or run became inactive. If inactive (terminal state ping
				// from markTerminalState), exit the goroutine instead of looping back.
				if !c.runHandle.IsActive() {
					logger.Debug(RELAYCLIENT, "", "Stoping relay  of messages for a run, run stopped being active")
					return
				}
				logger.Debug(RELAYCLIENT, "", "No more messages to relay at the moment, waiting for next notify")
				break
			}
			// Be careful to only run relayMessageViaGlobal in a new goroutine
			// If you also run the above in a go routine this would fuck up the automatic
			// comm order!
			go c.relayMessageViaGlobal(*outgoingMessage)
		}
	}
}

// relayMessageViaGlobal relays the message from the app to the global relay server.
func (c *RelayClient) relayMessageViaGlobal(outgoingMessage bridge.OutgoingMessage) {
	logger.Debug(RELAYCLIENT, "", "Sending out outgoing message")
	packets := make([]outgoingPacket, 0, 1)

	hasPrivacyProcessing := outgoingMessage.DpProps.Enabled || outgoingMessage.SmpcProps.Enabled
	logger.Debug(RELAYCLIENT, "", "relayMessageViaGlobal: hasPrivacyProcessing=%v (DP=%v SMPC=%v)", hasPrivacyProcessing, outgoingMessage.DpProps.Enabled, outgoingMessage.SmpcProps.Enabled)

	if hasPrivacyProcessing {
		// 1. Deserialize — source app never compresses, so always CompressionNone
		deserializedData, err := codec.Decode(outgoingMessage.Payload, nil, outgoingMessage.SrcSerializationMode, enums.CompressionNone)
		if err != nil {
			logger.Error(LOCAL, "", "failed to deserialize payload for privacy processing: %s", err.Error())
			return
		}

		// 2. Apply DP
		deserializedData, err = c.applyDp(outgoingMessage, deserializedData)
		if err != nil {
			logger.Error(LOCAL, "", "failed to apply DP: %s", err.Error())
			return
		}

		// 3. Apply SMPC: produces one packet per client shard
		if outgoingMessage.SmpcProps.Enabled {
			smpcPackets, err := c.applySmpc(outgoingMessage, deserializedData)
			if err != nil {
				logger.Error(LOCAL, "", "failed to apply SMPC: %s", err.Error())
				return
			}
			packets = smpcPackets
		} else {
			// DP only: re-encode with default serialization and compression
			encoded, err := codec.Encode(deserializedData, nil, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
			if err != nil {
				logger.Error(LOCAL, "", "failed to re-encode DP-processed data: %s", err.Error())
				return
			}
			pkt := c.buildPlainPacket(outgoingMessage, encoded, enums.DefaultSerializationMode, enums.DefaultCompressionMode, enums.DefaultSerializationMode, enums.DefaultCompressionMode)
			packets = append(packets, pkt)
		}
	} else {
		// No privacy processing: pass through the source payload as-is.
		// Wire serialization matches source — no transcoding needed.
		pkt := c.buildPlainPacket(outgoingMessage, outgoingMessage.Payload, outgoingMessage.SrcSerializationMode, enums.CompressionNone, outgoingMessage.SrcSerializationMode, enums.DefaultCompressionMode)
		packets = append(packets, pkt)
	}
	logger.Debug(RELAYCLIENT, "", "Outgoing message processed, will sent %d packets to global relay", len(packets))
	// Send all packets, each in its own goroutine.
	// Each goroutine transcodes (current → wire format) and encrypts before writing:
	//   dest != nil  → encrypt for that client
	//   dest nil, client role → encrypt for coordinator
	//   dest nil, coordinator role → no encryption (broadcast)
	var wg sync.WaitGroup
	for _, pkt := range packets {
		wg.Add(1)
		go func(p outgoingPacket) {
			defer wg.Done()
			c.sendPacket(p)
		}(pkt)
	}
	wg.Wait()
}

// sendPacket transcodes, encrypts, and writes one outgoing packet.
func (c *RelayClient) sendPacket(p outgoingPacket) {
	// Determine encryption target and poll for its public key.
	var encryptKey *shared_util.PublicKey
	var encryptFor *shared_link.ClientID
	if p.dest != nil && *p.dest != shared_link.ZERO_CLIENT_ID {
		logger.Debug(RELAYCLIENT, "", "Packet has specific destination client %s, will encrypt for that client", p.dest.ToString())
		encryptFor = p.dest
	} else if !c.runHandle.Meta.IsCoordinator {
		logger.Debug(RELAYCLIENT, "", "Packet has no specific destination and we're a client, will encrypt for coordinator %s", c.runHandle.Meta.CoordinatorID.ToString())
		coordID := c.runHandle.Meta.CoordinatorID
		encryptFor = &coordID
	}
	if encryptFor != nil {
		pubKey, ok := c.waitForPublicKey(*encryptFor)
		if !ok {
			logger.Warn(LOCAL, "", "run became inactive while waiting for public key of %x, aborting packet", *encryptFor)
			return
		}
		encryptKey = &pubKey
	}

	// Transcode from current to wire format (no-op when they match) and encrypt.
	// p is a value copy so overwriting p.payload is safe.
	logger.Debug(RELAYCLIENT, "", "Transcoding outgoing message")
	var err error
	p.payload, err = codec.DecodeEncode(p.payload, nil, p.currentSerializationMode, p.currentCompressionMode, encryptKey, p.wireSerializationMode, p.wireCompressionMode)
	if err != nil {
		logger.Error(LOCAL, "", "failed to transcode/encrypt payload: %s", err.Error())
		return
	}
	logger.Debug(RELAYCLIENT, "", "Outgoing message transcoded and encrypted")

	if err := c.writePacketBytes(p); err != nil {
		logger.Error(LOCAL, "", "failed to send packet: %s", err.Error())
	}
	logger.Debug(RELAYCLIENT, "", "Packet sent to global relay")
}

// buildPlainPacket creates a single outgoingPacket for a plain or P2P send.
// currentSerialization/currentCompression describe the payload's current encoding;
// wireSerialization/wireCompression describe the target wire format (goroutine transcodes if they differ).
func (c *RelayClient) buildPlainPacket(outgoingMessage bridge.OutgoingMessage, payload []byte, currentSerialization enums.SerializationMode, currentCompression enums.CompressionMode, wireSerialization enums.SerializationMode, wireCompression enums.CompressionMode) outgoingPacket {
	cmd := byte(shared_link.CMD_SEND_PLAIN_DATA)
	if outgoingMessage.DestinationClientID != nil && *outgoingMessage.DestinationClientID != shared_link.ZERO_CLIENT_ID {
		cmd = byte(shared_link.CMD_SEND_P2P_DATA)
	}
	return outgoingPacket{
		cmd:                      cmd,
		dest:                     outgoingMessage.DestinationClientID,
		currentSerializationMode: currentSerialization,
		wireSerializationMode:    wireSerialization,
		currentCompressionMode:   currentCompression,
		wireCompressionMode:      wireCompression,
		memoSize:                 outgoingMessage.MemoSize,
		memo:                     outgoingMessage.Memo,
		toAggregatorNameSize:     outgoingMessage.ToAggregatorNameSize,
		toAggregatorName:         outgoingMessage.ToAggregatorName,
		fromAggregatorNameSize:   outgoingMessage.FromAggregatorNameSize,
		fromAggregatorName:       outgoingMessage.FromAggregatorName,
		smpcExponent:             0,
		smpcOperation:            0,
		payload:                  payload,
	}
}

// writePacketBytes serializes the header and message to the global relay server.
// The write lock on globalConnection serializes concurrent goroutine sends.
func (c *RelayClient) writePacketBytes(p outgoingPacket) error {
	wireSerByte, err := p.wireSerializationMode.ToByte()
	if err != nil {
		return err
	}
	wireCompByte, err := p.wireCompressionMode.ToByte()
	if err != nil {
		return err
	}

	header := &shared_link.DataPacketHeader{
		Cmd:                     p.cmd,
		FromClientID:            c.runHandle.Meta.OwnClientId,
		DestinationClientID:     p.dest,
		SerializationMode:       byte(wireSerByte),
		CompressionMode:         byte(wireCompByte),
		MemoSize:                p.memoSize,
		MemoBytes:               p.memo,
		ToAggregatorNameSize:    p.toAggregatorNameSize,
		ToAggregatorNameBytes:   p.toAggregatorName,
		FromAggregatorNameSize:  p.fromAggregatorNameSize,
		FromAggregatorNameBytes: p.fromAggregatorName,
		SmpcExponentUsed:        p.smpcExponent,
		SmpcOperationUsed:       p.smpcOperation,
	}

	return c.globalConnection.WithHeaderWriteLock(func(io shared_link.TCPIOInterface) error {
		logger.Debug(RELAYCLIENT, "", "Sending header...")
		if err := shared_link.WriteHeader(io, header); err != nil {
			return err
		}
		logger.Debug(RELAYCLIENT, "", "Header sent, sending payload of length %d...", len(p.payload))
		if err := shared_link.WriteContentLength(io, uint64(len(p.payload))); err != nil {
			return err
		}
		return io.WriteBytes(p.payload)
	})
}

// waitForPublicKey blocks until the public key for clientID is available or the run becomes inactive.
// Returns (key, true) on success, (zero, false) if the run was torn down before the key arrived.
func (c *RelayClient) waitForPublicKey(clientID shared_link.ClientID) (shared_util.PublicKey, bool) {
	logger.Debug(RELAYCLIENT, "", "Waiting for public key of client %s to become available", clientID.ToString())
	for {
		if !c.runHandle.IsActive() {
			return shared_util.PublicKey{}, false
		}
		if pubKey, exists := c.runHandle.Meta.GetPublicKey(clientID); exists {
			return pubKey, true
		}
		time.Sleep(50 * time.Millisecond)
	}
}
