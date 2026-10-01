package bridge

import (
	"errors"
	"testing"

	enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
)

func newTestRun(t *testing.T) *FlRunMeta {
	t.Helper()
	flRun, err := NewFlRunStore().Create(2, enums.AppVersionV2)
	if err != nil {
		t.Fatalf("failed to create FL run: %v", err)
	}
	return flRun
}

func newTestCSR(t *testing.T, clientID shared.ClientID) []byte {
	t.Helper()
	_, csrPEM, err := util.GenerateClientCSR(clientID.ToString())
	if err != nil {
		t.Fatalf("failed to create CSR: %v", err)
	}
	return csrPEM
}

func TestSignClientCert_ClientAndCoordinator(t *testing.T) {
	flRun := newTestRun(t)
	for _, clientID := range append([]shared.ClientID{flRun.CoordinatorID}, flRun.ClientIDOrder...) {
		if _, err := flRun.SignClientCert(clientID, newTestCSR(t, clientID)); err != nil {
			t.Errorf("expected certificate for %s, got error: %v", clientID.ToString(), err)
		}
	}
}

func TestSignClientCert_OneKeyPerClient(t *testing.T) {
	flRun := newTestRun(t)
	clientID := flRun.ClientIDOrder[0]
	csrPEM := newTestCSR(t, clientID)

	if _, err := flRun.SignClientCert(clientID, csrPEM); err != nil {
		t.Fatalf("expected first signing to succeed, got: %v", err)
	}
	// e.g. a retry because the response got lost
	if _, err := flRun.SignClientCert(clientID, csrPEM); err != nil {
		t.Fatalf("expected signing the same key again to succeed, got: %v", err)
	}
	if _, err := flRun.SignClientCert(clientID, newTestCSR(t, clientID)); !errors.Is(err, ErrKeyAlreadyBound) {
		t.Fatalf("expected ErrKeyAlreadyBound for a second key, got: %v", err)
	}
}

func TestSignClientCert_Rejects(t *testing.T) {
	flRun := newTestRun(t)
	clientID := flRun.ClientIDOrder[0]

	unknownID, err := shared.NewClientID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flRun.SignClientCert(unknownID, newTestCSR(t, unknownID)); !errors.Is(err, ErrUnknownClient) {
		t.Errorf("expected ErrUnknownClient, got: %v", err)
	}

	// CSR of one client must not be signed for another one
	if _, err := flRun.SignClientCert(clientID, newTestCSR(t, flRun.ClientIDOrder[1])); !errors.Is(err, util.ErrInvalidCSR) {
		t.Errorf("expected ErrInvalidCSR for a CSR of another client, got: %v", err)
	}

	flRun.Stop()
	if _, err := flRun.SignClientCert(clientID, newTestCSR(t, clientID)); !errors.Is(err, shared.ErrStopped) {
		t.Errorf("expected ErrStopped for a stopped run, got: %v", err)
	}
}
