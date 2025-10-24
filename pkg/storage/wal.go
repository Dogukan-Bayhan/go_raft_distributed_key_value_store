package storage

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/tidwall/tinylru"
)

var (
	// ErrCorrupt Returns if the log is corrupt
	ErrCorrupt = errors.New("log corrupt")

	// ErrClosed is used when an operaiton cannot be completed
	// if given log is closed
	ErrClosed = errors.New("log closed")

	// If an entry is not found return ErrNotFound
	ErrNotFound = errors.New("not found")

	// ErrOutOfOrder is returned from Write() when the index is not equal to
	// LastIndex()+1. It's required that log monotonically grows by one and has
	// no gaps. Thus, the series 10,11,12,13,14 is valid, but 10,11,13,14 is
	// not because there's a gap between 11 and 13. Also, 10,12,11,13 is not
	// valid because 12 and 11 are out of order.
	ErrOutOfOrder = errors.New("out of order")

	// ErrOutOfRange is returned from TruncateFront() and TruncateBack() when
	// the index not in the range of the log's first and last index. Or, this
	// may be returned when the caller is attempting to remove *all* entries;
	// The log requires that at least one entry exists following a truncate.
	ErrOutOfRange = errors.New("out of range")

	// ErrEmptyLog is returned by Open() when the `AllowEmpty` option was not
	// provided and log has been emptied due to the use of TruncateFront() or
	// TruncateBack().
	ErrEmptyLog = errors.New("empty log")
)

// Format of the log file
type LogFormat byte

const (
	Binary LogFormat = 0

	JSON LogFormat = 1
)

// Options for Log
type Options struct {
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync bool
	// SegmentSize of each segment. This is just a target value, actual size
	// may differ. Default 20 MB
	SegmentSize int
	// LogFormat is the format of the log files. Default Binary
	LogFormat LogFormat
	// SegmentCacheSize is the maximum number of segments that will be held in
	// memory for caching. Increasing this value may enhance performance for
	// concurrent read operations. Default 1
	SegmentCacheSize int
	// NoCopy allows for the Read() operation to return the raw underlying data
	// slice. This is an optimization to help minimize allocations. When this
	// option is set, do not modify the returned data because it may affect
	// other Read calls. Default false
	NoCopy bool
	// AllowEmpty allows for a log to have all entries removed through the use
	// of TruncateFront() or TruncateBack(). Otherwise without this option,
	// at least one entry must always remain following a truncate operation.
	// Default false
	//
	// Warning: using this option changes the behavior of the log in the
	// following ways:
	// - An empty log will always have the FirstIndex() be equal to
	//   LastIndex()+1.
	// - For a newly created log that has no entries, FirstIndex() and
	//   LastIndex() return 1 and 0, respectively.
	//   Without AllowEmpty, both return 0.
	AllowEmpty bool
	// Perms represents the datafiles modes and permission bits
	DirPerms  os.FileMode
	FilePerms os.FileMode
}

var DefaultOptions = &Options{
	NoSync:           false,    // Fsync after every write
	SegmentSize:      20971520, // 20 MB log segment files
	LogFormat:        Binary,   // Binary format is small and fast
	SegmentCacheSize: 2,        // Number of cached in-memory segments
	NoCopy:           false,    // Make a new copy of data for every Read call
	AllowEmpty:       false,    // Do not allow empty log. 1+ entries required
	DirPerms:         0750,     // Permissions for the created directories
	FilePerms:        0640,     // Permissions for the created data files
}

type WAL struct {
	mu         sync.RWMutex
	path       string      // absolute path to log directory
	opts       Options     // log options
	closed     bool        // log is closed
	corrupt    bool        // log may be corrupt
	segments   []*segment  // all known log segments
	firstIndex uint64      // index of the first entry in log
	lastIndex  uint64      // index of the last entry in log
	sfile      *os.File    // tail segment file handle
	wbatch     Batch       // reusable write batch
	scache     tinylru.LRU // segment entries cache
}

// segment represents a single segment file.
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
	ebuf  []byte // cached entries buffer
	epos  []bpos // cached entries positions in buffer
}

type bpos struct {
	pos int // byte position
	end int // one byte past pos
}

// Batch of entries. Used to write multiple entries at once using WriteBatch().
type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	index uint64
	size  int
}

func Open(path string, opts *Options) (*WAL, error) {
	// If user don't give any option function use
	// default options as opts
	if opts == nil {
		opts = DefaultOptions
	}

	if opts.SegmentCacheSize <= 0 {
		opts.SegmentCacheSize = DefaultOptions.SegmentCacheSize
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultOptions.SegmentSize
	}
	if opts.DirPerms == 0 {
		opts.DirPerms = DefaultOptions.DirPerms
	}
	if opts.FilePerms == 0 {
		opts.FilePerms = DefaultOptions.FilePerms
	}

	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	w := &WAL{path: path, opts: *opts}
	w.scache.Resize(opts.SegmentCacheSize)

	err = os.MkdirAll(path, opts.DirPerms)
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(w.path)
	if err != nil {
		return nil, err
	}
	startIdx := -1
	endIdx := -1

	for _, fl := range files {
		name := fl.Name()
		if fl.IsDir() || len(name) < 20 {
			continue
		}

		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || index == 0 {
			continue
		}
		isStart := len(name) == 26 && strings.HasSuffix(name, ".START")
		isEnd := len(name) == 24 && strings.HasSuffix(name, ".END")
		if len(name) == 20 || isStart || isEnd {
			if isStart {
				startIdx = len(w.segments)
			} else if isEnd && endIdx == -1 {
				endIdx = len(w.segments)
			}
			w.segments = append(w.segments, &segment{
				index: index,
				path:  filepath.Join(w.path, name),
			})
		}
	}
	if len(w.segments) == 0 {
		w.segments = append(w.segments, &segment{
			index: 1,
			path:  filepath.Join(w.path, segmentName(1)),
		})

		w.firstIndex = 1
		w.lastIndex = 0
		w.sfile, err = os.OpenFile(w.segments[0].path,
			os.O_CREATE|os.O_RDWR|os.O_TRUNC, w.opts.FilePerms)
		return nil, err
	}

	if startIdx != -1 {
		if endIdx != -1 {
			return nil, ErrCorrupt
		}

		for i := 0; i < startIdx; i++ {
			err := os.Remove(w.segments[i].path)
			if err != nil {
				return nil, err
			}
		}

		w.segments = append([]*segment{}, w.segments[startIdx:]...)

		orgPath := w.segments[0].path
		finalPath := orgPath[:len(orgPath)-len(".START")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return nil, err
		}
		w.segments[0].path = finalPath
	}

	if endIdx != -1 {
		// Delete all files following END
		for i := len(w.segments) - 1; i > endIdx; i-- {
			if err := os.Remove(w.segments[i].path); err != nil {
				return nil, err
			}
		}
		w.segments = append([]*segment{}, w.segments[:endIdx+1]...)
		if len(w.segments) > 1 && w.segments[len(w.segments)-2].index ==
			w.segments[len(w.segments)-1].index {
			// remove the segment prior to the END segment because it shares
			// the same starting index.
			w.segments[len(w.segments)-2] = w.segments[len(w.segments)-1]
			w.segments = w.segments[:len(w.segments)-1]
		}
		// Rename the END segment
		orgPath := w.segments[len(w.segments)-1].path
		finalPath := orgPath[:len(orgPath)-len(".END")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return nil, err
		}
		w.segments[len(w.segments)-1].path = finalPath
	}

	w.firstIndex = w.segments[0].index

	lseg := w.segments[len(w.segments)-1]
	w.sfile, err = os.OpenFile(lseg.path, os.O_WRONLY, w.opts.FilePerms)

	if err != nil {
		return nil, err
	}
	if _, err := w.sfile.Seek(0, 2); err != nil {
		return nil, err
	}
	// Load the last segment entries
	if err := w.loadSegmentEntries(lseg); err != nil {
		return err
	}
	w.lastIndex = lseg.index + uint64(len(lseg.epos)) - 1
	if w.lastIndex > 0 && w.firstIndex > w.lastIndex && !w.opts.AllowEmpty {
		return nil, ErrEmptyLog
	}

	return w, nil
}

func abs(path string) (string, error) {
	if path == ":memory:" {
		return "", errors.New("in-memory log not supported")
	}
	return filepath.Abs(path)
}


// pushCache adds the semgnet at segIdx to the in-memory LRU segment cache.
//
// This function is invoked whenever a segment is rotated
// so that recently used segments remain accessible from memory for fast reads.
//
// Internally it uses tinylru to manage a limited-size cache of segments.
// If adding a new segment causes an old one to be evicted (due to LRU capacity), 
// the evicted segment's in-memory buffers are released to minimize memory usage.
func (w *WAL) pushCache(segIdx int) {
	_, _, _, v, evicted :=
		w.scache.SetEvicted(segIdx, w.segments[segIdx])
	if evicted {
		s := v.(*segment)
		s.ebuf = nil
		s.epos = nil
	}
}


// segmentName returns the standardized filename for a WAL segment.
//
// Each segment is named using a 20-digit zero-padded integer (e.g., "00000000000000000001").
// This naming convention ensures correct lexical ordering when reading from disk
// (so that segment #2 always appears after segment #1 even in string sort order).
func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}


// Close safely closes the WAL, flushing all buffered data to disk.
//
// The method performs a final fsync() to guarantee durability, then closes the
// active segment file handle. It also ensures that the WAL cannot be written to
// again after closure.
//
// Error handling:
//   - If already closed: returns ErrClosed
//   - If corruption detected: returns ErrCorrupt
//   - If fsync or file close fails: returns the underlying I/O error
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Prevent double-close or further writes.
	if w.closed {
		if w.corrupt {
			return ErrCorrupt
		}
		return ErrClosed
	}

	// Ensure on-disk durability for all prior writes.
	if err := w.sfile.Sync(); err != nil {
		return err
	}
	// Close the current segment file handle.
	if err := w.sfile.Close(); err != nil {
		return err
	}

	// Mark WAL as closed.
	w.closed = true
	if w.corrupt {
		return ErrCorrupt
	}
	return nil
}

// Write appends a single entry to the WAL.
//
// This method wraps the lower-level batch writer with a single-entry batch for simplicity.
// The entry will be written atomically with its index and data, and flushed according to
// the WAL options (fsync depends on NoSync).
//
// Concurrency:
//   - Lock-protected to prevent concurrent writes.
//   - Safe against corruption and closed states.
//
// Error cases:
//   - ErrCorrupt if WAL previously marked as corrupt.
//   - ErrClosed if WAL already closed.
func (w *WAL) Write(index uint64, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.corrupt {
		return ErrCorrupt
	} else if w.closed {
		return ErrClosed
	}

	// Reuse a pre-allocated batch structure to minimize allocations.
	w.wbatch.Clear()
	w.wbatch.Write(index, data)

	// Perform the actual write operation to the segment.
	return w.writeBatch(&w.wbatch)
}


// addpendEntry serializes a single WAL entry into the provided byte slice.
//
// Depending on the configured log format (binary or JSON), this method delegates
// to the appropriate encoder (appendBinaryEntry or appendJSONEntry) and returns
// both the updated buffer and the entry’s byte position (bpos).
func (w *WAL) addpendEntry(dst []byte, index uint64, data []byte) (out []byte, epos bpos) {
	if w.opts.LogFormat == JSON {
		return appendJSONEntry(dst, index, data)
	}
	return appendBinaryEntry(dst, data)
}



// cycle safely closes the current WAL segment and starts a new one.
//
// This function is typically invoked when the active segment reaches
// the configured size limit (SegmentSize) or during log compaction.
//
// Steps:
//   1. Flush pending writes to disk via fsync (durability guarantee).
//   2. Close the current segment file handle.
//   3. Push the just-closed segment into the LRU cache for potential reuse.
//   4. Create a new segment starting at (lastIndex + 1).
//   5. Open the new segment file with read/write permissions and truncate mode.
//   6. Append this new segment to the list of known WAL segments.
func (w *WAL) cycle() error {
	var err error

	err = w.sfile.Sync()
	if err != nil {
		return err
	}

	if err := w.sfile.Close(); err != nil {
		return err
	}

	w.pushCache(len(w.segments) - 1)
	s := &segment {
		index: w.lastIndex + 1,
		path:  filepath.Join(w.path, segmentName(w.lastIndex+1)),
	}
	w.sfile, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, w.opts.FilePerms)

	if err != nil {
		return err
	}
	w.segments = append(w.segments, s)
	return nil
}

// appendJSONEntry serializes a single WAL entry into JSON format.
//
// The resulting representation is of the form:
//   {"index":"42","data":{"key":"value"}}\n
//
// It appends this encoded entry to the existing byte buffer (dst),
// and returns both the updated buffer and the entry’s byte position (bpos).
func appendJSONEntry(dst []byte, index uint64, data []byte) (out []byte, epos bpos) {
	pos := len(dst)
	dst = append(dst, `{"index":"`...)
	dst = strconv.AppendUint(dst, index, 10)
	dst = append(dst, `","data":`...)
	dst = appendJSONData(dst, data)
	dst = append(dst, '}', '\n')
	return dst, bpos{pos, len(dst)}
}


// appendJSONData appends the encoded form of `s` into the destination buffer `dst`.
// It ensures that the appended data is always JSON-safe.
//
// Behavior:
//   • If `s` is valid UTF-8 → store it as a JSON string with prefix "+" (text mode)
//   • If `s` is not valid UTF-8 → store it as base64 with prefix "$" (binary mode)
//
// Prefixes help distinguish later whether the stored data should be interpreted
// as text or binary when reading back.
//
// Example:
//   Input:  s = []byte("hello")
//   Output: `" +hello"`
//   Input:  s = []byte{0x01, 0x02}
//   Output: `" $AQI="`
func appendJSONData(dst []byte, s []byte) []byte {
	if utf8.Valid(s) {
		b, _ := json.Marshal(*(*string)(unsafe.Pointer(&s)))
		dst = append(dst, '"', '+')
		return append(dst, b[1:]...)
	}
	dst = append(dst, '"', '$')
	dst = append(dst, base64.URLEncoding.EncodeToString(s)...)
	return append(dst, '"')
}
