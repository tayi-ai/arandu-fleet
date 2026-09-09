package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// The actions a worker accepts. They are the whole vocabulary of the node API.
const (
	ActionStatus      = "status"
	ActionDiagnostics = "diagnostics"
	ActionCollective  = "collective"
)

// Job is what a worker is asked to run: one run id, one action.
type Job struct {
	ID     string `json:"id"`
	Action string `json:"action"`
}

// Worker is the node API as the control plane sees it.
//
// An interface so the service can be driven against fakes in tests, and so a
// second transport cannot arrive with its own idea of what a submission is.
type Worker interface {
	// Status reads what the node reports about itself and its current job.
	Status(ctx context.Context, n Node) ([]byte, error)
	// Submit hands the node a job. An accepted submission means the process
	// started, not that it finished: HTTP 202 is not completion.
	Submit(ctx context.Context, n Node, job Job) ([]byte, error)
	// Cancel asks the node to stop the run with that id.
	Cancel(ctx context.Context, n Node, job Job) ([]byte, error)
}

// WorkerPort is where every node listens on its private address.
const WorkerPort = "8787"

// maxAnswer caps what is read from a node: a worker answers in a few hundred
// bytes, and a node that answers more is a node something else is speaking for.
const maxAnswer = 8192

// HTTPWorker talks to the node API over the private network with a bearer
// token. The token is read from the application's configuration and never
// written anywhere by this package.
type HTTPWorker struct {
	Token  string
	Client *http.Client
}

// NewHTTPWorker returns a client with the timeout the node API is designed for.
func NewHTTPWorker(token string) (*HTTPWorker, error) {
	if token == "" {
		return nil, errors.New("cluster: the worker token is empty; refusing to address the fleet without one")
	}
	return &HTTPWorker{Token: token, Client: &http.Client{Timeout: 15 * time.Second}}, nil
}

func (w *HTTPWorker) Status(ctx context.Context, n Node) ([]byte, error) {
	return w.call(ctx, n, http.MethodGet, "/status", nil)
}

func (w *HTTPWorker) Submit(ctx context.Context, n Node, job Job) ([]byte, error) {
	body, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	return w.call(ctx, n, http.MethodPost, "/jobs", body)
}

func (w *HTTPWorker) Cancel(ctx context.Context, n Node, job Job) ([]byte, error) {
	body, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	return w.call(ctx, n, http.MethodPost, "/cancel", body)
}

func (w *HTTPWorker) call(ctx context.Context, n Node, method, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+net.JoinHostPort(n.IP, WorkerPort)+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+w.Token)
	req.Header.Set("Content-Type", "application/json")
	res, err := w.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cluster: node %s: %w", n.ID, err)
	}
	defer res.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(res.Body, maxAnswer))
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		return answer, fmt.Errorf("cluster: node %s answered HTTP %d: %s", n.ID, res.StatusCode, answer)
	}
	return answer, nil
}
