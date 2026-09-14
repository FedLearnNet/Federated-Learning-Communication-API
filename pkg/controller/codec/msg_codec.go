package codec

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fc_controller/pkg/controller/enums"
	util "fc_controller/pkg/shared/util"
	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"
	"io"
)

// Decode decrypts, decompresses, and Deserializes incoming data according to
// If a nil pointer is given for the decrypt key, no decryption is performed.
// Similarly, if the compression mode is "none", no decompression is performed.
func Decode(data []byte, decryptKey *util.PrivateKey, srcSerialization enums.SerializationMode, srcCompression enums.CompressionMode) (interface{}, error) {
	var err error
	// Decrypt
	if decryptKey != nil {
		data, err = util.Decrypt(data, decryptKey)
		if err != nil {
			return nil, err
		}
	}

	// Deserialize
	var deserializedData interface{}
	err = DeserializeToInterface(data, &deserializedData, srcSerialization, srcCompression)
	if err != nil {
		return nil, err
	}

	return deserializedData, nil
}

// Encode serializes, compresses, and encrypts outgoing data.
// Pass nil for encryptKey to skip encryption.
func Encode(data interface{}, encryptKey *util.PublicKey, targetSerialization enums.SerializationMode, targetCompression enums.CompressionMode) ([]byte, error) {
	out, err := SerializeInterface(data, targetSerialization, targetCompression)
	if err != nil {
		return nil, err
	}
	if encryptKey != nil {
		out, err = util.Encrypt(*encryptKey, out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// DecodeEncode decodes an incoming message and re-encodes it for outgoing transmission.
// Useful for transcoding between serialization formats, changing compression, or adding/removing encryption.
// If serialization and compression are unchanged, skips the deserialize/reserialize round-trip.
// Encrypt and decrypt use different keys and must always be performed.
// If nil pointers are given for the decrypt/encrypt keys, the respective operation is skipped.
// If srcSerialization is SerializationGolang, data must be an interface{} value (not []byte);
// decryptKey must be nil and srcCompression must be CompressionNone in that case.
func DecodeEncode(data interface{}, decryptKey *util.PrivateKey, srcSerialization enums.SerializationMode, srcCompression enums.CompressionMode, encryptKey *util.PublicKey, targetSerialization enums.SerializationMode, targetCompression enums.CompressionMode) ([]byte, error) {
	var err error
	var out []byte

	if srcSerialization == enums.SerializationGolang {
		if decryptKey != nil {
			return nil, errors.New("decryptKey must be nil when srcSerialization is SerializationGolang")
		}
		if srcCompression != enums.CompressionNone {
			return nil, errors.New("srcCompression must be CompressionNone when srcSerialization is SerializationGolang")
		}
		out, err = SerializeInterface(data, targetSerialization, targetCompression)
		if err != nil {
			return nil, err
		}
	} else {
		bts, ok := data.([]byte)
		if !ok {
			return nil, errors.New("data is not []byte for non-golang serialization mode")
		}

		if decryptKey != nil {
			bts, err = util.Decrypt(bts, decryptKey)
			if err != nil {
				return nil, err
			}
		}

		if srcCompression == targetCompression && srcSerialization == targetSerialization {
			out = bts
		} else if srcSerialization == targetSerialization {
			bts, err = DecompressBytes(bts, srcCompression)
			if err != nil {
				return nil, err
			}
			out, err = CompressBytes(bts, targetCompression)
			if err != nil {
				return nil, err
			}
		} else {
			var deserializedData interface{}
			err = DeserializeToInterface(bts, &deserializedData, srcSerialization, srcCompression)
			if err != nil {
				return nil, err
			}
			out, err = SerializeInterface(deserializedData, targetSerialization, targetCompression)
			if err != nil {
				return nil, err
			}
		}
	}

	if encryptKey != nil {
		out, err = util.Encrypt(*encryptKey, out)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func SerializeInterface(data interface{}, serialization enums.SerializationMode, compression enums.CompressionMode) ([]byte, error) {
	var serializedData []byte
	var err error

	switch serialization {
	case enums.SerializationJSON:
		serializedData, err = serializeInterfaceToJSON(data, compression)
	case enums.SerializationCBOR:
		serializedData, err = serializeInterfaceToCBOR(data, compression)
	case enums.SerializationPickle:
		err = errors.New("pickle serialization is not supported in Go; cannot encode message")
	default:
		return nil, errors.New("unsupported serialization mode")
	}

	if err != nil {
		return nil, err
	}
	return serializedData, nil
}

func DeserializeToInterface(serializedData []byte, target interface{}, serialization enums.SerializationMode, compression enums.CompressionMode) error {
	var err error

	switch serialization {
	case enums.SerializationJSON:
		err = deserializeJSONToInterface(serializedData, target, compression)
	case enums.SerializationCBOR:
		err = deserializeCBORToInterface(serializedData, target, compression)
	case enums.SerializationPickle:
		err = errors.New("pickle deserialization is not supported in Go; cannot decode message")
	default:
		return errors.New("unsupported serialization mode")
	}

	return err
}

func serializeInterfaceToJSON(data interface{}, compression enums.CompressionMode) ([]byte, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return CompressBytes(encoded, compression)
}

func deserializeJSONToInterface(jsonData []byte, target interface{}, compression enums.CompressionMode) error {
	decoded, err := DecompressBytes(jsonData, compression)
	if err != nil {
		return err
	}
	return json.Unmarshal(decoded, target)
}

func serializeInterfaceToCBOR(data interface{}, compression enums.CompressionMode) ([]byte, error) {
	encoded, err := cbor.Marshal(data)
	if err != nil {
		return nil, err
	}
	return CompressBytes(encoded, compression)
}

func deserializeCBORToInterface(cborData []byte, target interface{}, compression enums.CompressionMode) error {
	decoded, err := DecompressBytes(cborData, compression)
	if err != nil {
		return err
	}
	return cbor.Unmarshal(decoded, target)
}

func CompressBytes(data []byte, compression enums.CompressionMode) ([]byte, error) {
	switch compression {
	case enums.CompressionNone:
		return data, nil
	case enums.CompressionGzip:
		return gzipCompress(data)
	case enums.CompressionZstd:
		return zstdCompress(data)
	default:
		return nil, errors.New("unsupported compression mode")
	}
}

func DecompressBytes(data []byte, compression enums.CompressionMode) ([]byte, error) {
	switch compression {
	case enums.CompressionNone:
		return data, nil
	case enums.CompressionGzip:
		return gzipDecompress(data)
	case enums.CompressionZstd:
		return zstdDecompress(data)
	default:
		return nil, errors.New("unsupported compression mode")
	}
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func zstdCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func zstdDecompress(data []byte) ([]byte, error) {
	reader, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
