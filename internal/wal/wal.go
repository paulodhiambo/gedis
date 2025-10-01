package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type SyncPolicy int

const (
	SyncNone SyncPolicy = iota
	SyncPeriodic
	SyncImmediate
)

const (
	OpPut  = byte(1)
	OpDel  = byte(2)
	OpMeta = byte(3)
)

const (
	defaultBatchSize    = 256
	defaultBatchTimeout = 5 * time.Millisecond
	defaultChanBuffer   = 65536
	defaultSegMaxBytes  = 64 << 20 // 64MB
)

var (
	ErrWALClosed     = errors.New("wal closed")
	ErrReplayCorrupt = errors.New("wal replay: corrupt record")
)

type Record struct {
	Seq uint64 // monotonic sequence assigned at append time
	Op  byte
	Key []byte
	Val []byte
}

// opReq is internal per-operation request structure
type opReq struct {
	rec  *Record
	done chan error
}

// WALConfig holds runtime knobs
type WALConfig struct {
	Dir           string
	PerShard      bool // if true, create per-shard writers
	NumShards     int  // only used if PerShard
	BatchSize     int
	BatchTimeout  time.Duration
	ChanBuffer    int
	SyncPolicy    SyncPolicy
	SegMaxBytes   int64
	SegmentPrefix string // default "wal-"
}

type WAL struct {
	cfg WALConfig

	globalSeq uint64 // atomically incremented at append to provide ordering within writer
	writers   []*walWriter

	closed int32
	wg     sync.WaitGroup
}

// NewWAL creates WAL with config. dir must exist.
func NewWAL(cfg WALConfig) (*WAL, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = defaultBatchTimeout
	}
	if cfg.ChanBuffer <= 0 {
		cfg.ChanBuffer = defaultChanBuffer
	}
	if cfg.SegMaxBytes <= 0 {
		cfg.SegMaxBytes = defaultSegMaxBytes
	}
	if cfg.SegmentPrefix == "" {
		cfg.SegmentPrefix = "wal-"
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("wal dir required")
	}
	if cfg.PerShard && cfg.NumShards <= 0 {
		return nil, fmt.Errorf("num shards > 0 required when PerShard")
	}

	w := &WAL{cfg: cfg}
	writerCount := 1
	if cfg.PerShard {
		writerCount = cfg.NumShards
	}
	w.writers = make([]*walWriter, writerCount)

	for i := 0; i < writerCount; i++ {
		ww, err := newWalWriter(cfg.Dir, cfg.SegmentPrefix, cfg.SegMaxBytes, cfg.BatchSize, cfg.BatchTimeout, cfg.ChanBuffer, cfg.SyncPolicy)
		if err != nil {
			// close previously opened writers
			for j := 0; j < i; j++ {
				w.writers[j].closeSync() // best-effort
			}
			return nil, err
		}
		w.writers[i] = ww
		w.wg.Add(1)
		go func(ww *walWriter) {
			ww.run()
			w.wg.Done()
		}(ww)
	}
	return w, nil
}

// Append appends a record for a particular writer index (shardIdx) if per-shard mode.
// If PerShard=false, ignore shardIdx and use writer 0.
// If done==nil, append is fire-and-forget; if not nil, blockable notification occurs.
func (w *WAL) Append(shardIdx int, op byte, key, val []byte, done chan error) (uint64, error) {
	if atomic.LoadInt32(&w.closed) == 1 {
		return 0, ErrWALClosed
	}
	seq := atomic.AddUint64(&w.globalSeq, 1)
	rec := &Record{Seq: seq, Op: op, Key: append([]byte(nil), key...), Val: append([]byte(nil), val...)}
	req := &opReq{rec: rec, done: done}

	target := 0
	if w.cfg.PerShard {
		target = shardIdx % len(w.writers)
	}
	// attempt to push to channel; block if necessary (backpressure)
	w.writers[target].enqueue(req)
	return seq, nil
}

// AppendSync appends and waits until write is durable according to sync policy.
func (w *WAL) AppendSync(shardIdx int, op byte, key, val []byte) (uint64, error) {
	done := make(chan error, 1)
	seq, err := w.Append(shardIdx, op, key, val, done)
	if err != nil {
		return 0, err
	}
	if err := <-done; err != nil {
		return seq, err
	}
	return seq, nil
}

// Close gracefully shuts down writers.
func (w *WAL) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return ErrWALClosed
	}
	for _, ww := range w.writers {
		ww.close()
	}
	// wait goroutines
	w.wg.Wait()
	return nil
}

// Replay walks all segments (across writers) and invokes apply(rec) in increasing segment & seq order.
// For per-shard, we replay each writer independently; if global ordering required, you must maintain a global writer.
func (w *WAL) Replay(apply func(r *Record) error) error {
	// iterate writers
	for i, ww := range w.writers {
		if err := ww.replay(apply); err != nil {
			return fmt.Errorf("writer %d: %w", i, err)
		}
	}
	return nil
}

type walWriter struct {
	dir          string
	prefix       string
	segMaxBytes  int64
	batchSize    int
	batchTimeout time.Duration
	chanBuffer   int
	syncPolicy   SyncPolicy

	mu      sync.Mutex
	curFile *os.File
	writer  *bufio.Writer
	curSize int64

	in  chan *opReq
	out chan struct{}

	closing chan struct{}
	closed  chan struct{}

	segSeq uint64
}

func newWalWriter(dir, prefix string, segMaxBytes int64, batchSize int, batchTimeout time.Duration, chanBuffer int, syncPolicy SyncPolicy) (*walWriter, error) {
	ww := &walWriter{
		dir:          dir,
		prefix:       prefix,
		segMaxBytes:  segMaxBytes,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		chanBuffer:   chanBuffer,
		syncPolicy:   syncPolicy,
		in:           make(chan *opReq, chanBuffer),
		out:          make(chan struct{}, 1),
		closing:      make(chan struct{}),
		closed:       make(chan struct{}),
	}
	// open or create latest segment file
	if err := ww.openNextSegment(); err != nil {
		return nil, err
	}
	return ww, nil
}

func (ww *walWriter) enqueue(req *opReq) {
	ww.in <- req
}

func (ww *walWriter) close() {
	close(ww.closing)
	<-ww.closed
}

func (ww *walWriter) closeSync() {
	// best effort immediate close
	select {
	case <-ww.closed:
		return
	default:
		close(ww.closing)
		<-ww.closed
	}
}

func (ww *walWriter) run() {
	defer close(ww.closed)
	var batch []*opReq
	timer := time.NewTimer(ww.batchTimeout)
	defer timer.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		ww.mu.Lock()
		// write each record
		for _, r := range batch {
			if err := ww.writeRecord(r.rec); err != nil {
				if r.done != nil {
					r.done <- err
				}
				continue
			}
		}
		// flush
		if err := ww.writer.Flush(); err != nil {
			for _, r := range batch {
				if r.done != nil {
					r.done <- err
				}
			}
			ww.mu.Unlock()
			batch = batch[:0]
			return
		}
		// sync according to policy
		if ww.syncPolicy == SyncImmediate || ww.syncPolicy == SyncPeriodic {
			if err := ww.curFile.Sync(); err != nil {
				for _, r := range batch {
					if r.done != nil {
						r.done <- err
					}
				}
				ww.mu.Unlock()
				batch = batch[:0]
				return
			}
		}
		// success acknowledge
		for _, r := range batch {
			if r.done != nil {
				r.done <- nil
			}
		}
		ww.mu.Unlock()
		// rotate if needed
		if ww.curSize >= ww.segMaxBytes {
			ww.rotateSegment()
		}
		batch = batch[:0]
	}

loop:
	for {
		select {
		case req := <-ww.in:
			batch = append(batch, req)
			if len(batch) >= ww.batchSize {
				flushBatch()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(ww.batchTimeout)
			}
		case <-timer.C:
			flushBatch()
			timer.Reset(ww.batchTimeout)
		case <-ww.closing:
			flushBatch()
			// close file
			ww.mu.Lock()
			ww.writer.Flush()
			ww.curFile.Sync()
			ww.curFile.Close()
			ww.mu.Unlock()
			break loop
		}
	}
}

func (ww *walWriter) writeRecord(r *Record) error {
	// prepare buffer: [uint64 seq][byte op][uint32 klen][uint32 vlen][k][v][uint32 crc]
	var b bytes.Buffer
	if err := binary.Write(&b, binary.LittleEndian, r.Seq); err != nil {
		return err
	}
	if err := b.WriteByte(r.Op); err != nil {
		return err
	}
	if err := binary.Write(&b, binary.LittleEndian, uint32(len(r.Key))); err != nil {
		return err
	}
	if err := binary.Write(&b, binary.LittleEndian, uint32(len(r.Val))); err != nil {
		return err
	}
	if _, err := b.Write(r.Key); err != nil {
		return err
	}
	if _, err := b.Write(r.Val); err != nil {
		return err
	}
	// crc over header+payload
	crc := crc32.ChecksumIEEE(b.Bytes())
	if err := binary.Write(&b, binary.LittleEndian, crc); err != nil {
		return err
	}
	// write to file with mu already held by caller
	n, err := ww.writer.Write(b.Bytes())
	if err != nil {
		return err
	}
	ww.curSize += int64(n)
	return nil
}

func (ww *walWriter) openNextSegment() error {
	// determine next seq by scanning existing files
	// simple approach: pick timestamp-based name to avoid global seq detection complexity
	ww.mu.Lock()
	defer ww.mu.Unlock()
	name := fmt.Sprintf("%s%020d.log", ww.prefix, time.Now().UnixNano())
	path := filepath.Join(ww.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	ww.curFile = f
	ww.writer = bufio.NewWriterSize(f, 1<<20)
	// start size from file stat
	if fi, err := f.Stat(); err == nil {
		ww.curSize = fi.Size()
	} else {
		ww.curSize = 0
	}
	return nil
}

func (ww *walWriter) rotateSegment() error {
	ww.mu.Lock()
	defer ww.mu.Unlock()
	// flush & close current
	ww.writer.Flush()
	ww.curFile.Sync()
	ww.curFile.Close()
	// open new segment
	return ww.openNextSegment()
}

// replay reads all segment files for this writer (by prefix) in lexical order and applies records
// The apply callback must be fast; if you need to apply to a store, keep apply simple (apply in-memory).
func (ww *walWriter) replay(apply func(r *Record) error) error {
	// list files that match prefix
	files, err := filepath.Glob(filepath.Join(ww.dir, ww.prefix+"*.log"))
	if err != nil {
		return err
	}
	// sort lexical order - filenames include timestamp so lex order ~ time order
	for _, fpath := range files {
		if err := ww.replayFile(fpath, apply); err != nil {
			if errors.Is(err, ErrReplayCorrupt) {
				// stop at corruption
				return err
			}
			return err
		}
	}
	return nil
}

func (ww *walWriter) replayFile(path string, apply func(r *Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		// read header: seq(8) op(1) klen(4) vlen(4)
		var seq uint64
		if err := binary.Read(rd, binary.LittleEndian, &seq); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		op, err := rd.ReadByte()
		if err != nil {
			return err
		}
		var klen uint32
		var vlen uint32
		if err := binary.Read(rd, binary.LittleEndian, &klen); err != nil {
			return err
		}
		if err := binary.Read(rd, binary.LittleEndian, &vlen); err != nil {
			return err
		}
		key := make([]byte, klen)
		if _, err := io.ReadFull(rd, key); err != nil {
			return ErrReplayCorrupt
		}
		val := make([]byte, vlen)
		if vlen > 0 {
			if _, err := io.ReadFull(rd, val); err != nil {
				return ErrReplayCorrupt
			}
		}
		// read crc
		var crc uint32
		if err := binary.Read(rd, binary.LittleEndian, &crc); err != nil {
			return ErrReplayCorrupt
		}
		// recompute crc over header+payload (seq..val)
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, seq)
		buf.WriteByte(op)
		binary.Write(&buf, binary.LittleEndian, klen)
		binary.Write(&buf, binary.LittleEndian, vlen)
		buf.Write(key)
		if vlen > 0 {
			buf.Write(val)
		}
		if crc32.ChecksumIEEE(buf.Bytes()) != crc {
			return ErrReplayCorrupt
		}
		rec := &Record{Seq: seq, Op: op, Key: key, Val: val}
		if err := apply(rec); err != nil {
			return err
		}
	}
}
