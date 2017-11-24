package utils

import (
	"github.com/forchain/blockchainparser"
	"github.com/piotrnar/gocoin/lib/btc"
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"
	"os"
	"log"
	"compress/gzip"
	"regexp"
	"strconv"
	"runtime"
	"bufio"
	"strings"
	"sort"
)

type tOutput struct {
	addr string // index
	val  int64  // val
}

//  (index -> output)
type tOutputMap map[uint32]tOutput

// tx -> tOutputMap
type tUnspentMap map[string]tOutputMap

// add -> balance
type tBalanceMap map[string]int64

type tFile2Blocks struct {
	file   uint32
	blocks []*blockchainparser.Block
}

type BalanceExporter struct {
	blockNO_ uint32
	fileNO_  int
	dataDir_ string
	magicId_ blockchainparser.MagicId
	outDir_  string

	unspentMap_ tUnspentMap
	balanceMap_ tBalanceMap

	snapshot_ uint32

	file2blocksMap_ map[uint32][]*blockchainparser.Block

	file2blocksCh_ chan *tFile2Blocks

	fileList_ []int
}

func (_b *BalanceExporter) loadUnspent(_path string, _wg *sync.WaitGroup) {
	defer _wg.Done()

	filename := fmt.Sprintf("%v/unspent.gz", _path)
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1000*1024*1024)
	for scanner.Scan() {
		l := scanner.Text()
		if tokens := strings.Split(l, ","); len(tokens) == 2 {
			txID := tokens[0]
			outputs := tokens[1:]

			out := make(tOutputMap)
			for _, output := range outputs {
				if tokens := strings.Split(output, " "); len(tokens) == 3 {
					if index, err := strconv.Atoi(tokens[0]); err == nil {
						addr := tokens[1]
						if val, err := strconv.Atoi(tokens[2]); err == nil {
							out[uint32(index)] = tOutput{
								addr,
								int64(val),
							}
						}
					}
				}
			}
			_b.unspentMap_[txID] = out
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("loaded", filename)
}

func (_b *BalanceExporter) loadBalance(_path string, _wg *sync.WaitGroup) {
	defer _wg.Done()

	filename := fmt.Sprintf("%v/balance.gz", _path)

	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 100*1024*1024)
	for scanner.Scan() {
		l := scanner.Text()

		if tokens := strings.Split(l, " "); len(tokens) == 2 {
			addr := tokens[0]
			if balance, err := strconv.Atoi(tokens[1]); err == nil {
				_b.balanceMap_[addr] = int64(balance)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("loaded", filename)
}

func (_b *BalanceExporter) loadMap() {
	// tx -> (index -> {address, value})
	_b.unspentMap_ = make(tUnspentMap)
	// address -> balance
	_b.balanceMap_ = make(tBalanceMap)

	if files, err := ioutil.ReadDir(_b.outDir_); err == nil && len(files) > 0 {
		start := 0
		var fi os.FileInfo
		for _, f := range files {
			if f.IsDir() {
				r, err := regexp.Compile("(\\d+)\\.(\\d+)") // Do we have an 'N' or 'index' at the beginning?
				if err != nil {
					log.Println(err)
					break
				}
				if matches := r.FindStringSubmatch(f.Name()); len(matches) == 3 {
					if fileNO, err := strconv.Atoi(matches[1]); err == nil {
						if blockNO, err := strconv.Atoi(matches[2]); err == nil {
							if uint32(blockNO) < _b.blockNO_ {
								if blockNO > start {
									start = blockNO
									fi = f
									_b.fileNO_ = fileNO
								}
							} else if uint32(blockNO) == _b.blockNO_ {
								start = blockNO
								fi = f
								_b.fileNO_ = fileNO
								break
							}
						}
					}
				}
			}
		}

		if start > 0 {
			wg := new(sync.WaitGroup)
			wg.Add(2)
			path := fmt.Sprintf("%v/%v", _b.outDir_, fi.Name())
			go _b.loadUnspent(path, wg)
			go _b.loadBalance(path, wg)

			wg.Wait()
		}
	}
}

func (_b *BalanceExporter) Export(_blockNO uint32, _snapshot uint32, _dataDir string, _magicId blockchainparser.MagicId, _outDir string) {
	cpuNum := uint32(runtime.NumCPU())

	_b.blockNO_ = _blockNO
	_b.dataDir_ = _dataDir
	_b.magicId_ = _magicId
	_b.outDir_ = _outDir
	_b.snapshot_ = _snapshot
	_b.file2blocksCh_ = make(chan *tFile2Blocks, cpuNum)
	_b.fileList_ = make([]int, 0)
	_b.file2blocksMap_ = make(map[uint32][]*blockchainparser.Block)
	_b.fileNO_ = -1

	if files, err := ioutil.ReadDir(_dataDir + "/blocks/"); err == nil {
		for _, f := range files {
			r, err := regexp.Compile("blk(\\d+)\\.dat") // Do we have an 'N' or 'index' at the beginning?
			if err != nil {
				log.Println(err)
				break
			}

			if matches := r.FindStringSubmatch(f.Name()); len(matches) == 2 {
				if fileNO, err := strconv.Atoi(matches[1]); err == nil && fileNO > _b.fileNO_ {
					_b.fileList_ = append(_b.fileList_, fileNO)
				}
			}
		}

		log.Print("Start Export: files ", len(_b.fileList_), ",CPU ", cpuNum)

		sort.Ints(_b.fileList_)

		_b.loadMap()

		waitProcess := new(sync.WaitGroup)
		waitProcess.Add(1)
		go _b.processFile(waitProcess)

		waitLoad := new(sync.WaitGroup)
		i := 0
		for i < len(_b.fileList_) {
			fileNO := uint32(_b.fileList_[i])
			waitLoad.Add(1)
			go _b.loadFile(waitLoad, fileNO)
			i++
			if uint32(i)%cpuNum == 0 {
				waitLoad.Wait()
			}
		}
		waitLoad.Wait()
		waitProcess.Wait()

		log.Print("balance number:", len(_b.balanceMap_))
		log.Print("unspent number:", len(_b.unspentMap_))
	}
}

func (_b *BalanceExporter) saveUnspent(_wg *sync.WaitGroup, _path string) {
	defer _wg.Done()
	fileName := fmt.Sprintf("%v/unspent.gz", _path)

	b := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	bb := new(bytes.Buffer)
	for tx, outputs := range _b.unspentMap_ {
		bb.WriteString(tx)
		for i, o := range outputs {
			l := fmt.Sprintf(",%v %v %v", i, o.addr, o.val)
			bb.WriteString(l)
		}
		bb.WriteByte('\n')
		w.Write([]byte(bb.Bytes()))
		bb.Reset()
	}

	w.Close()
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}
	log.Println("saved", fileName)
}

type tSortedBalance []string

func (s tSortedBalance) Len() int {
	return len(s)
}
func (s tSortedBalance) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s tSortedBalance) Less(i, j int) bool {
	// remove trailing return
	t1 := strings.Split(s[i][:len(s[i])-1], " ")
	t2 := strings.Split(s[j][:len(s[j])-1], " ")
	if len(t1) == 2 && len(t2) == 2 {
		if v1, err := strconv.ParseUint(t1[1], 10, 0); err == nil {
			if v2, err := strconv.ParseUint(t2[1], 10, 0); err == nil {
				return v1 > v2
			}
		}
	}

	return len(s[i]) < len(s[j])
}

func (_b *BalanceExporter) saveBalance(_wg *sync.WaitGroup, _path string) {
	defer _wg.Done()

	fileName := fmt.Sprintf("%v/balance.gz", _path)

	b := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	// if OOM, try delete map item then append to list
	sorted := make(tSortedBalance, 0)

	for k, v := range _b.balanceMap_ {
		line := fmt.Sprintln(k, v)
		sorted = append(sorted, line)
	}
	sort.Sort(sorted)
	for _, v := range sorted {
		w.Write([]byte(v))
	}

	w.Close()
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}
	log.Println("saved", fileName)
}

func (_b *BalanceExporter) saveMap(_files uint32, _blockNum uint32) {
	wg := new(sync.WaitGroup)
	wg.Add(2)

	path := fmt.Sprintf("%v/%v.%v", _b.outDir_, _files, _blockNum)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0755)
	}

	go _b.saveBalance(wg, path)
	go _b.saveUnspent(wg, path)

	wg.Wait()
}

func (_b *BalanceExporter) loadFile(wg *sync.WaitGroup, _fileNO uint32) {
	defer wg.Done()

	offset := uint32(8)
	startPos := uint32(0)

	filepath := fmt.Sprintf(_b.dataDir_+"/blocks/blk%05d.dat", _fileNO)
	fi, err := os.Stat(filepath)
	if err != nil {
		log.Fatal(err)
	}
	// get the size
	size := uint32(fi.Size())
	//size /= 5
	// Open file for reading
	blockFile, err := blockchainparser.NewBlockFile(_b.dataDir_, _fileNO)
	if err != nil {
		log.Fatal(err)
	}

	defer blockFile.Close()

	if err != nil {
		log.Fatal(err)
	}

	blocks := make([]*blockchainparser.Block, 0)
	for startPos < size {
		block, err := blockchainparser.ParseBlockFromFile(blockFile, _b.magicId_)
		if err != nil {
			log.Println(err)
			break
		}
		blocks = append(blocks, block)
		startPos += block.Length + offset
	}

	log.Println("loaded file", _fileNO, len(_b.file2blocksMap_), len(blocks))

	_b.file2blocksCh_ <- &tFile2Blocks{_fileNO, blocks}
}

func (_b *BalanceExporter) processFile(_wg *sync.WaitGroup) {
	defer _wg.Done()

	n := 0
	for n < len(_b.fileList_) {
		fileNO := uint32(_b.fileList_[n])
		if blocks, ok := _b.file2blocksMap_[fileNO]; ok {
			for _, b := range blocks {
				for _, t := range b.Transactions {
					txID := t.Txid().String()
					for _, i := range t.Vin {
						if int32(i.Index) >= 0 {
							hash := i.Hash.String()
							if unspent, ok := _b.unspentMap_[hash]; ok {
								if o, ok := unspent[i.Index]; ok {
									delete(unspent, i.Index)
									if len(unspent) == 0 {
										delete(_b.unspentMap_, hash)
									}

									balance := _b.balanceMap_[o.addr]
									balance -= o.val
									if balance <= 0 {
										delete(_b.balanceMap_, o.addr)
									} else {
										_b.balanceMap_[o.addr] = balance
									}
								}
							}
						}
					}

					for i, o := range t.Vout {
						if a := btc.NewAddrFromPkScript(o.Script, false); a != nil && o.Value > 0 {
							index := uint32(i)
							addr := a.String()
							balance := _b.balanceMap_[addr] + o.Value
							_b.balanceMap_[addr] = balance
							unspent, ok := _b.unspentMap_[txID]
							if !ok {
								unspent = make(tOutputMap)
							}
							unspent[index] = tOutput{addr, o.Value}
							_b.unspentMap_[txID] = unspent
						}
					}
				}
			}

			delete(_b.file2blocksMap_, fileNO)
			n++

			blockNum := uint32(len(blocks))
			log.Println("processed file", fileNO, len(_b.file2blocksMap_), blockNum)

			if uint32(n)%_b.snapshot_ == 0 {
				_b.saveMap(fileNO, blockNum)
			}
		} else {
			f2b := <-_b.file2blocksCh_
			_b.file2blocksMap_[f2b.file] = f2b.blocks
			log.Println("received file", f2b.file, len(_b.file2blocksMap_), len(f2b.blocks))
		}
	}

}
