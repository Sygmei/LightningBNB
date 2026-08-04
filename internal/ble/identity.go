package ble

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const serverIDPrefix = "lbnb:"

type ServerID [16]byte

func NewServerID() (ServerID, error) {
	var id ServerID
	if _, err := rand.Read(id[:]); err != nil {
		return ServerID{}, err
	}
	// Keep the familiar UUID representation while reserving the lbnb: prefix
	// to distinguish this application identity from platform BLE identifiers.
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return id, nil
}

func ParseServerID(value string) (ServerID, error) {
	if !strings.HasPrefix(strings.ToLower(value), serverIDPrefix) {
		return ServerID{}, errors.New("server ID must start with lbnb:")
	}
	value = value[len(serverIDPrefix):]
	compact := strings.ReplaceAll(value, "-", "")
	if len(compact) != 32 {
		return ServerID{}, errors.New("server ID must contain a 128-bit UUID")
	}
	decoded, err := hex.DecodeString(compact)
	if err != nil {
		return ServerID{}, fmt.Errorf("decode server ID: %w", err)
	}
	var id ServerID
	copy(id[:], decoded)
	return id, nil
}

func (id ServerID) String() string {
	encoded := hex.EncodeToString(id[:])
	return serverIDPrefix + encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
