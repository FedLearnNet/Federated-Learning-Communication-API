// app <-- [CONTROLLER] <-- global relay server
// Contains all code related to handling messages
// from the global relay server (Which received the message from another client/aggregators local controller)
// to this local controller
// Then offloads the message to the local app handler
package relaycomm

import (
	"errors"
	"fc_controller/pkg/controller/bridge"
	"fc_controller/pkg/controller/enums"
	shared_link "fc_controller/pkg/shared/link"
	logger "fc_controller/pkg/shared/logger"
	shared_util "fc_controller/pkg/shared/util"
	"fmt"
)

func (c *RelayClient) readIncomming() {
	for {
		err := c.readHandleMessage()
		if err != nil {
			if errors.Is(err, shared_link.ErrStopped) {
				return // deliberate stop via MarkFinished
			}
			// Any non-stop error on a TCP stream is permanent — there is no reconnect logic,
			// so every subsequent read will keep failing. Transition to error state so all
			// components (watchOutgoing, AppCommunicatorV2 handlers) observe the cascade.
			logger.Error(LOCAL, "", "Permanent error reading from global relay, marking run as errored: %s", err.Error())
			c.runHandle.MarkError()
			return
		}
	}
}

// readHandleMessage reads and dispatches one message from the global relay server.
// [GLOBAL] --> [LOCAL]
// All messages from the global relay server are just relayed messages from other
// local relay servers
func (s *RelayClient) readHandleMessage() error {
	// Acquire the read lock and hold it for the full header+data read so no
	// interleaved reads from a concurrent goroutine corrupt the stream.
	unlock, err := s.globalConnection.LockHeaderRead()
	if err != nil {
		return fmt.Errorf("failed to lock global connection for reading: %w", err)
	}
	cmd, err := s.globalConnection.ReadByte()
	if err != nil {
		unlock()
		return fmt.Errorf("failed to read command byte: %w", err)
	}
	header, dataBytes, err := s.readMessage(cmd)
	unlock()
	if err != nil {
		return fmt.Errorf("failed to read message from global connection: %w", err)
	}

	// Offload processing so we can return to reading immediately.
	go s.handleMessage(cmd, header, dataBytes)
	return nil
}

// readMessage reads the header and payload for one wire message.
// Caller must hold the read lock around this call.
func (c *RelayClient) readMessage(cmd byte) (interface{}, []byte, error) {
	if cmd == shared_link.CMD_SEND_PUBLIC_KEYS {
		header, err := shared_link.ReadPublicKeyBundleHeader(c.globalConnection, cmd)
		if err != nil {
			return header, nil, err
		}
		// no payload for public key bundles
		return header, nil, nil
	} else if shared_link.IsDataMessageCmd(cmd) {
		header, err := shared_link.ReadDataHeader(c.globalConnection, cmd)
		if err != nil {
			return header, nil, err
		}
		dataBytes, err := c.readDataBytes(header.ContentLength)
		if err != nil {
			return header, nil, err
		}
		return header, dataBytes, nil
	} else {
		return nil, nil, fmt.Errorf("unsupported command received from global relay server: %d", cmd)
	}
}

// handleMessage dispatches one fully-read wire message to the correct handler.
func (c *RelayClient) handleMessage(cmd byte, header interface{}, dataBytes []byte) {
	publicKeyHeader, dataPacketHeader := c.headerInterfaceToPointers(header)

	if cmd == shared_link.CMD_SEND_PUBLIC_KEYS {
		if publicKeyHeader == nil {
			logger.Error(LOCAL, "", "Incorrect header received for public key announcement message")
			return
		}
		c.handlePublicKeysMessage(publicKeyHeader)
	} else if shared_link.IsDataMessageCmd(cmd) {
		if dataPacketHeader == nil {
			logger.Error(LOCAL, "", "Incorrect header received for data message")
			return
		}
		switch cmd {
		case shared_link.CMD_SEND_PLAIN_DATA, shared_link.CMD_SEND_P2P_DATA:
			c.MessageToAppHandler(dataPacketHeader, dataBytes)
		case shared_link.CMD_SEND_SMPC_DATA:
			if err := c.handleReceivedSMPCDataShard(
				dataPacketHeader.FromClientID,
				dataPacketHeader.MemoString,
				dataPacketHeader.SerializationMode,
				dataPacketHeader.CompressionMode,
				dataPacketHeader.SmpcExponentUsed,
				enums.SMPCOperationByte(dataPacketHeader.SmpcOperationUsed),
				dataBytes,
				dataPacketHeader,
			); err != nil {
				logger.Error(LOCAL, "", "failed to handle SMPC data shard: %s", err.Error())
			}
		case shared_link.CMD_SEND_SMPC_AGG:
			if err := c.handleReceivedSMPCAggregatedData(
				dataPacketHeader.MemoString,
				dataBytes,
				dataPacketHeader.SerializationMode,
				dataPacketHeader.CompressionMode,
				dataPacketHeader.FromClientID,
				dataPacketHeader.SmpcExponentUsed,
				enums.SMPCOperationByte(dataPacketHeader.SmpcOperationUsed),
				dataPacketHeader,
			); err != nil {
				logger.Error(LOCAL, "", "failed to handle SMPC aggregated data: %s", err.Error())
			}
		default:
			logger.Error(LOCAL, "", "Unsupported data message command received from global relay server: %d", cmd)
		}
	} else {
		logger.Error(LOCAL, "", "Unsupported command received from global relay server: %d", cmd)
	}
}

func (c *RelayClient) handlePublicKeysMessage(header *shared_link.PublicKeyBundleHeader) {
	logger.Info(LOCAL, "", "Received public key announcement for %d clients", header.NumEntries)
	if err := c.runHandle.Meta.SetPublicKeys(header.Entries); err != nil {
		logger.Error(LOCAL, "", "Failed to update public keys from global relay server announcement: %s", err.Error())
	}
}

// MessageToAppHandler decrypts an incoming plain or P2P message and delivers it to
// the run's incoming message store for the app communicator to pick up.
func (c *RelayClient) MessageToAppHandler(header *shared_link.DataPacketHeader, dataBytes []byte) {
	meta := c.runHandle.Meta
	fromCoordinator := header.FromClientID == meta.CoordinatorID

	// Can only receive plain message from coordinator or as the coordinator
	// A client2client sends via CMD_SEND_P2P_DATA
	if header.Cmd == shared_link.CMD_SEND_PLAIN_DATA && !fromCoordinator && !meta.IsCoordinator {
		logger.Warn(LOCAL, "", "Received plain message from non-coordinator client %s, ignoring", header.FromClientID.ToString())
		return
	}

	// Decrypt when the message was encrypted for us:
	//   - P2P messages are always encrypted for their destination.
	//   - Otherwise only the coordinator does not encrypt messages when broadcasting
	shouldDecrypt := header.Cmd == shared_link.CMD_SEND_P2P_DATA || !fromCoordinator
	var deliverBytes []byte
	if shouldDecrypt {
		privKey := meta.OwnPrivateKey
		decrypted, err := shared_util.Decrypt(dataBytes, &privKey)
		if err != nil {
			logger.Error(LOCAL, "", "failed to decrypt incoming message from %x: %s", header.FromClientID, err.Error())
			return
		}
		deliverBytes = decrypted
	} else {
		deliverBytes = dataBytes
	}

	// Reject messages carrying the golang-only serialization mode — it is never valid on the wire.
	if enums.SerializationModeByte(header.SerializationMode) == enums.SerializationGolangByte {
		logger.Error(LOCAL, "", "illegal serialization mode on wire: %d", header.SerializationMode)
		return
	}

	err := c.AddMessage(
		enums.SerializationModeByte(header.SerializationMode),
		enums.CompressionModeByte(header.CompressionMode),
		header, deliverBytes)
	if err != nil {
		logger.Error(LOCAL, "", "failed to validate incoming message from global relay server: %s", err.Error())
		return
	}
}

// AddMessage stores an incoming data message into the run's message store.
// Comm ID validation (duplicates, stale auto IDs) is handled inside SafeIncomingStore.
func (c *RelayClient) AddMessage(srcSerializationMode enums.SerializationModeByte, srcCompressionMode enums.CompressionModeByte,
	header *shared_link.DataPacketHeader, data interface{}) error {
	incomingMsg := bridge.NewIncomingMessage(
		srcSerializationMode,
		srcCompressionMode,
		header,
		data,
	)

	if !c.runHandle.SafeIncomingStore(incomingMsg) {
		logger.Warn(LOCAL, "", "Run marked inactive or validation failed, discarding incoming message from %s", header.FromClientID.ToString())
		return nil
	}

	// Wake the app communicator (non-blocking — it drains all pending messages on each wake).
	select {
	case c.runHandle.IncomingNotify <- struct{}{}:
	default:
	}
	return nil
}

// headerInterfaceToPointers type-asserts the header interface returned by readMessage.
// Caller must check that the expected pointer is non-nil before using it.
func (c *RelayClient) headerInterfaceToPointers(header interface{}) (*shared_link.PublicKeyBundleHeader, *shared_link.DataPacketHeader) {
	var publicKeyBundleHeader *shared_link.PublicKeyBundleHeader
	var dataPacketHeader *shared_link.DataPacketHeader
	switch h := header.(type) {
	case *shared_link.PublicKeyBundleHeader:
		publicKeyBundleHeader = h
	case *shared_link.DataPacketHeader:
		dataPacketHeader = h
	default:
		logger.Error(LOCAL, "", "Received unsupported header type from global relay server: %T", header)
	}
	return publicKeyBundleHeader, dataPacketHeader
}

func (s *RelayClient) readDataBytes(totalBytes uint64) ([]byte, error) {
	data := make([]byte, totalBytes)
	if err := s.globalConnection.ReadBytes(data); err != nil {
		return nil, err
	}
	return data, nil
}
