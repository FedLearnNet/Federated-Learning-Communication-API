package http

import (
	"bytes"
	"encoding/json"
	nethttp "net/http"
	"net/http/httptest"
	"testing"

	bridge "fc_controller/pkg/relayserver/bridge"
	enums "fc_controller/pkg/shared/enums"
	shared "fc_controller/pkg/shared/link"
	"fc_controller/pkg/shared/util"
)

func postSign(t *testing.T, s *RelayServiceHTTP, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleSignFLRunCert(rec, httptest.NewRequest(nethttp.MethodPost, "/sign-fl-run-cert", bytes.NewReader(body)))
	return rec
}

func signBody(t *testing.T, channel string, clientID shared.ClientID, csrPEM []byte) []byte {
	t.Helper()
	body, err := json.Marshal(signFLRunCertRequest{Channel: channel, ClientID: clientID, CSR: string(csrPEM)})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestHandleSignFLRunCert(t *testing.T) {
	store := bridge.NewFlRunStore()
	s := NewHTTPServer(store)
	flRun, err := store.Create(2, enums.AppVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	channel := flRun.Channel.ToString()
	clientID := flRun.ClientIDOrder[0]
	_, csrPEM, err := util.GenerateClientCSR(clientID.ToString())
	if err != nil {
		t.Fatal(err)
	}
	_, otherKeyCSRPEM, err := util.GenerateClientCSR(clientID.ToString())
	if err != nil {
		t.Fatal(err)
	}
	unknownID, err := shared.NewClientID()
	if err != nil {
		t.Fatal(err)
	}
	_, unknownCSRPEM, err := util.GenerateClientCSR(unknownID.ToString())
	if err != nil {
		t.Fatal(err)
	}

	rec := postSign(t, s, signBody(t, channel, clientID, csrPEM))
	if rec.Code != nethttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp signFLRunCertResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil || resp.Certificate == "" {
		t.Fatalf("expected a certificate in the response, got %+v (err: %v)", resp, err)
	}

	cases := map[string]struct {
		body []byte
		want int
	}{
		"invalid JSON":           {[]byte("not json"), nethttp.StatusBadRequest},
		"invalid channel":        {signBody(t, "nope", clientID, csrPEM), nethttp.StatusBadRequest},
		"unknown channel":        {signBody(t, shared.NewChannel().ToString(), clientID, csrPEM), nethttp.StatusNotFound},
		"unknown client":         {signBody(t, channel, unknownID, unknownCSRPEM), nethttp.StatusNotFound},
		"invalid CSR":            {signBody(t, channel, clientID, []byte("not a csr")), nethttp.StatusBadRequest},
		"CSR of another client":  {signBody(t, channel, flRun.ClientIDOrder[1], csrPEM), nethttp.StatusBadRequest},
		"second key for client":  {signBody(t, channel, clientID, otherKeyCSRPEM), nethttp.StatusConflict},
		"same key again (retry)": {signBody(t, channel, clientID, csrPEM), nethttp.StatusOK},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if rec := postSign(t, s, tc.body); rec.Code != tc.want {
				t.Errorf("expected %d, got %d: %s", tc.want, rec.Code, rec.Body.String())
			}
		})
	}
}
