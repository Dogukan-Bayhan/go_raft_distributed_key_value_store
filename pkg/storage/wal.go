package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

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



func Open(path string, opts *Options) (*WAL, error){
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
	if(err != nil) {
		return nil, err
	}
	w := &WAL{path: path, opts: *opts}
	w.scache.Resize(opts.SegmentCacheSize)


	err = os.MkdirAll(path, opts.DirPerms)
	if(err != nil) {
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
			path: filepath.Join(w.path, segmentName(1)),
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

		for i:= 0; i < startIdx; i++ {
			err := os.Remove(w.segments[i].path);
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

	lseg := w.segments[len(w.segments) - 1]
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

func (w *WAL) pushCache(segIdx int) {
	_, _, _, v, evicted :=
		w.scache.SetEvicted(segIdx, w.segments[segIdx])
	if evicted {
		s := v.(*segment)
		s.ebuf = nil
		s.epos = nil
	}
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		if w.corrupt {
			return ErrCorrupt
		}
		return ErrClosed
	}
	if err := w.sfile.Sync(); err != nil {
		return err
	}
	if err
}