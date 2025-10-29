package loader

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"strings"
	"time"
)

func NewCsvLoaderV2(path string, cacheCap int) (*CSVLoader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	l := &CSVLoader{
		Path:      path,
		f:         f,
		Cache:     make(map[int][]string),
		cap:       cacheCap,
		edits:     make(map[int]map[int]string),
		requestCh: make(chan int, 100),
		stopCh:    make(chan struct{}),
		Offsets:   make([]int64, 0),
		OffBuilt:  false,
	}
	// 后台完整构建偏移（非阻塞）
	//go l.buildOffsetsAsyncV2()
	l.buildOffsetsAsyncV2()
	return l, nil
}

func (l *CSVLoader) buildOffsetsAsyncV2() {
	l.TryLock()
	defer l.Mu.Unlock()
	r := bufio.NewReader(l.f)

	for i := 0; ; i++ {
		var buf bytes.Buffer
		line, _, err := r.ReadLine()
		if err != nil && err != io.EOF {
			l.ErrMsg = err.Error()
			return
		}
		if err == io.EOF {
			l.OffBuilt = true
			break
		}
		buf.Write(line)
		// TODO 处理多行字段/异步处理
		record, err := csv.NewReader(bytes.NewReader(buf.Bytes())).Read()
		if err != nil {
			l.Cache[i] = []string{strings.TrimSpace(buf.String())}
		} else {
			l.Cache[i] = record
		}

	}
}

func (l *CSVLoader) readRowByOffsetNoCacheV2() {
	//var line
	//l.TryRLock()
	//csv.NewReader(bytes.NewReader())

}

func (l *CSVLoader) TryRLock() {
	for !l.Mu.TryRLock() {
		time.Sleep(time.Millisecond * 10)
	}
}

func (l *CSVLoader) TryLock() {
	for !l.Mu.TryLock() {
		time.Sleep(time.Millisecond * 10)
	}
}

func (l *CSVLoader) TryLockFunc(f func() bool) bool {
	l.TryRLock()
	defer l.Mu.RUnlock()
	return f()
}

func (l *CSVLoader) GetRowV2(row int) []string {
	l.TryRLock()
	defer l.Mu.RUnlock()
	if data, ok := l.Cache[row]; ok {
		return data
	}
	return nil
}
