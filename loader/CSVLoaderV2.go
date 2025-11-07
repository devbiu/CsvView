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

		CacheStart:  0,
		CacheEnd:    0,
		ActiveIndex: 0,
		maxMemory:   1 * 1024, // default 1mb
		loadRatio:   0.3,
		cleanRatio:  0.5,
	}

	// 后台完整构建偏移（非阻塞）
	//go l.buildOffsetsAsyncV2()
	l.buildOffsetsAsyncV2(true)
	//go func() {
	//	for {
	//		time.Sleep(1 * time.Second)
	//		l.TryRLock()
	//		num := 0
	//		for i := range l.Cache {
	//			num += cap(l.Cache[i])
	//		}
	//		l.Mu.RUnlock()
	//		log.Printf("initial cache built, total rows: %d, total memory used: %d bytes\n", len(l.Cache), num)
	//		time.Sleep(1 * time.Second)
	//	}
	//}()
	return l, nil
}

func (l *CSVLoader) buildOffsetsAsyncV2(next bool) {
	l.TryLock()
	//defer l.Mu.Unlock()
	r := bufio.NewReader(l.f)
	use := 0
	// TODO 往上读如何解决
	for i := l.CacheEnd; ; {
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
			l.Cache[int(i)] = []string{strings.TrimSpace(buf.String())}
		} else {
			l.Cache[int(i)] = record
		}
		//l.CacheEnd = i + 1
		use += cap(l.Cache[int(i)])
		if float64(use)/float64(l.maxMemory) > l.loadRatio {
			break
		}
		if next {
			i++
			l.CacheEnd += 1
		} else {
			i--
			l.CacheStart -= 1
		}

	}
	l.Mu.Unlock()
	//// 添加完毕之后清理内存
	//ratio := l.adaptiveTrimRatio()
	//if ratio > 0 {
	//	l.trimCache(ratio)
	//}
}

// TODO 清理的有问题
func (l *CSVLoader) trimCache(ratio float64) {
	l.TryRLock()
	// 计算需要清理的行数
	numToTrim := int(float64(l.CacheEnd-l.CacheStart) * ratio)
	l.Mu.RUnlock()
	if numToTrim <= 0 {
		return
	}
	// 清理最早的行
	l.TryLock()
	defer l.Mu.Unlock()
	cut := int64(float64(len(l.Cache)) / ratio)
	if l.ActiveIndex-l.CacheStart > l.CacheEnd-l.ActiveIndex {
		//// 活跃行在后半部分，清理前半部分
		for i := l.CacheStart; i < l.CacheStart+cut; i++ {
			delete(l.Cache, int(i))
		}
		l.CacheStart += cut
	} else {
		// 活跃行在前半部分，清理后半部分
		for i := l.CacheEnd - cut; i < l.CacheEnd; i++ {
			delete(l.Cache, int(i))
		}
		l.CacheEnd -= cut
	}

}

func (l *CSVLoader) adaptiveTrimRatio() float64 {
	l.TryRLock()
	defer l.Mu.RUnlock()
	use := 0
	// TODO 取消遍历， 通过其他方式获取内存占用
	for i := l.CacheStart; i < l.CacheEnd; i++ {
		use += cap(l.Cache[int(i)])
	}
	ratio := float64(use) / float64(l.maxMemory)
	switch {
	case ratio < l.loadRatio:
		return -2.0
	case ratio < 1-l.loadRatio:
		return 0.0
	case ratio < 1.1:
		return l.cleanRatio / 2
	case ratio < 1+l.loadRatio:
		return l.cleanRatio
	default:
		return l.cleanRatio * 2
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

func (l *CSVLoader) GetRowV2(row int) ([]string, bool) {
	l.TryRLock()
	defer l.Mu.RUnlock()

	update := false
	if row > 0 {
		// 判断是否需要加载更多数据
		if int64(row) > l.ActiveIndex && row >= int(float64(l.CacheEnd)*0.8) {
			go l.buildOffsetsAsyncV2(true)
			update = true
		} else if int64(row) < l.ActiveIndex && row <= int(float64(l.CacheStart)*0.2) {
			go l.buildOffsetsAsyncV2(false)
			update = true
		}
	}

	if data, ok := l.Cache[row]; ok {
		// TODO 是否需要替换为写锁
		l.ActiveIndex = int64(row)
		return data, update
	}
	return nil, update
}
