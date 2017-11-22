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
	"sync/atomic"
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

type BalanceExporter struct {
	blockNO_ uint32
	fileNO_  uint32
	dataDir_ string
	magicId_ blockchainparser.MagicId
	outDir_  string

	blocks_ uint32

	unspentMap_ tUnspentMap
	balanceMap_ tBalanceMap

	txOutCh_  chan string
	txInCh_   chan *blockchainparser.Transaction
	snapshot_ uint32
}

func (be *BalanceExporter) loadUnspent(_path string, _wg *sync.WaitGroup) {
	defer _wg.Done()

	filename := fmt.Sprintf("%v/unspent.gz", _path)
	log.Println("loading", filename)
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
			be.unspentMap_[txID] = out
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("loaded", filename)
}

func (be *BalanceExporter) loadBalance(_path string, _wg *sync.WaitGroup) {
	defer _wg.Done()

	filename := fmt.Sprintf("%v/balance.gz", _path)

	log.Println("loading", filename)
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
				be.balanceMap_[addr] = int64(balance)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("loaded", filename)
}

func (be *BalanceExporter) loadMap() {
	// tx -> (index -> {address, value})
	be.unspentMap_ = make(tUnspentMap)
	// address -> balance
	be.balanceMap_ = make(tBalanceMap)

	if files, err := ioutil.ReadDir(be.outDir_); err == nil && len(files) > 0 {
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
							if uint32(blockNO) < be.blockNO_ {
								if blockNO > start {
									start = blockNO
									fi = f
									be.fileNO_ = uint32(fileNO)
								}
							} else if uint32(blockNO) == be.blockNO_ {
								start = blockNO
								fi = f
								be.fileNO_ = uint32(fileNO)
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
			path := fmt.Sprintf("%v/%v", be.outDir_, fi.Name())
			go be.loadUnspent(path, wg)
			go be.loadBalance(path, wg)

			wg.Wait()
		}
	}
}

func (be *BalanceExporter) Export(_blockNO uint32, _snapshot uint32, _dataDir string, _magicId blockchainparser.MagicId, _outDir string) {
	be.blockNO_ = _blockNO
	be.dataDir_ = _dataDir
	be.magicId_ = _magicId
	be.outDir_ = _outDir
	be.txOutCh_ = make(chan string)
	be.txInCh_ = make(chan *blockchainparser.Transaction)
	be.snapshot_ = _snapshot

	wg := new(sync.WaitGroup)

	be.loadMap()
	go be.processTx()

	i := uint32(0)
	cpuNum := uint32(runtime.NumCPU())
	if files, err := ioutil.ReadDir(_dataDir + "/blocks/"); err == nil {
		log.Print("Start Export: files ", len(files)/2, ",CPU ", cpuNum)
		for _, f := range files {
			r, err := regexp.Compile("blk(\\d+)\\.dat") // Do we have an 'N' or 'index' at the beginning?
			if err != nil {
				log.Println(err)
				break
			}
			if matches := r.FindStringSubmatch(f.Name()); len(matches) == 2 {
				if fileNO, err := strconv.Atoi(matches[1]); err == nil && uint32(fileNO) > be.fileNO_ {
					wg.Add(1)
					go be.exportUnspent(wg, uint32(fileNO))
					i++
					if i%be.snapshot_ == 0 {
						wg.Wait()
						be.saveMap(uint32(fileNO))
					}
					if i%cpuNum == 0 {
						wg.Wait()
					}
				}
			}
		}
	}
	wg.Wait()
	close(be.txInCh_)
	log.Print("balance number:", len(be.balanceMap_))
	log.Print("unspent number:", len(be.unspentMap_))
}

func (be *BalanceExporter) saveUnspent(_wg *sync.WaitGroup, _path string) {
	defer _wg.Done()
	fileName := fmt.Sprintf("%v/unspent.gz", _path)
	log.Println("saving", fileName)

	b := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	bb := new(bytes.Buffer)
	for tx, outputs := range be.unspentMap_ {
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

func (be *BalanceExporter) saveBalance(_wg *sync.WaitGroup, _path string) {
	defer _wg.Done()

	fileName := fmt.Sprintf("%v/balance.gz", _path)
	log.Println("saving", fileName)

	b := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	// if OOM, try delete map item then append to list
	sorted := make(tSortedBalance, 0)

	for k, v := range be.balanceMap_ {
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

func (be *BalanceExporter) saveMap(_files uint32) {
	wg := new(sync.WaitGroup)
	wg.Add(2)

	blockNO := atomic.LoadUint32(&be.blocks_)
	path := fmt.Sprintf("%v/%v.%v", be.outDir_, _files, blockNO)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0755)
	}

	go be.saveBalance(wg, path)
	go be.saveUnspent(wg, path)

	wg.Wait()
}

func (be *BalanceExporter) processTx() {
outer:
	for t := range be.txInCh_ {
		txID := t.Txid().String()
		for k, i := range t.Vin {
			if int32(i.Index) >= 0 {
				if len(i.Hash) > 256 || len(i.Hash) <= 0 {
					log.Println("Invalid tx input", txID, k)
					continue outer
				}
				hash := i.Hash.String()
				unspent, ok1 := be.unspentMap_[hash]
				if !ok1 {
					unspent = make(tOutputMap)
					be.unspentMap_[hash] = unspent
				}
				addr2val, ok2 := unspent[i.Index]
				if !ok2 {
					unspent[i.Index] = tOutput{}
				} else {
					delete(unspent, i.Index)
					if len(unspent) == 0 {
						delete(be.unspentMap_, hash)
					}

					balance := be.balanceMap_[addr2val.addr]
					balance -= addr2val.val
					if balance <= 0 {
						delete(be.balanceMap_, addr2val.addr)
					} else {
						be.balanceMap_[addr2val.addr] = balance
					}
				}
			}
		}

		unspent, okTx := be.unspentMap_[txID]
		if !okTx {
			unspent = make(tOutputMap)
		}
		for i, o := range t.Vout {
			addr := btc.NewAddrFromPkScript(o.Script, false)
			if addr != nil {
				index := uint32(i)
				if okTx {
					if _, okOut := unspent[index]; okOut {
						delete(unspent, index)
						continue
					}
				}

				balance := be.balanceMap_[addr.String()] + o.Value
				be.balanceMap_[addr.String()] = balance
				unspent[index] = tOutput{addr.String(), o.Value}
			}
		}
		if len(unspent) > 0 {
			be.unspentMap_[txID] = unspent
		} else if okTx {
			delete(be.unspentMap_, txID)
		}

		be.txOutCh_ <- txID
	}
}

func (be *BalanceExporter) exportUnspent(wg *sync.WaitGroup, _fileNum uint32) {

	defer wg.Done()

	offset := uint32(8)
	startPos := uint32(0)

	filepath := fmt.Sprintf(be.dataDir_+"/blocks/blk%05d.dat", _fileNum)
	fi, err := os.Stat(filepath)
	if err != nil {
		log.Fatal(err)
	}
	// get the size
	size := uint32(fi.Size())
	//size /= 5
	// Open file for reading
	blockFile, err := blockchainparser.NewBlockFile(be.dataDir_, _fileNum)
	if err != nil {
		log.Fatal(err)
	}

	defer blockFile.Close()

	if err != nil {
		log.Fatal(err)
	}

	for startPos < size {
		block, err := blockchainparser.ParseBlockFromFile(blockFile, be.magicId_)
		if err != nil {
			log.Println(err)
			break
		}

		for _, t := range block.Transactions {
			be.txInCh_ <- &t
			<-be.txOutCh_
		}
		atomic.AddUint32(&be.blocks_, 1)

		startPos += block.Length + offset
	}
	log.Println(blockFile.FileNum)
}
