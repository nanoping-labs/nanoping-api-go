package guide

import (
	"io"
	"sync"
)

// lockedWriter lets the steps and the streams they watch write to the same
// output.
type lockedWriter struct {
	mutex sync.Mutex
	out   io.Writer
}

func (w *lockedWriter) Write(data []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.out.Write(data)
}
