package utils

import (
	"fmt"
	"os"
	"bytes"
	"github.com/piotrnar/gocoin/lib/btc"
	"github.com/forchain/blockchainparser"
	"log"
	"sync"
	"compress/gzip"
	"io/ioutil"
	"time"
)

func ProcessTx(txCh chan *blockchainparser.Transaction, resCh chan int) {
	// tx -> (index -> {address, value})
	unspentMap := make(map[string]map[uint32]struct {
		string
		int64
	})
	// address -> balance
	balanceMap := make(map[string]int64)

outer:
	for t := range txCh {
		txID := t.Txid().String()
		for k, i := range t.Vin {
			if i.Index != 4294967295 {
				if len(i.Hash) > 256 || len(i.Hash) <= 0 {
					//log.Fatalln("Invalid hash", len(i.Hash), txID)
					log.Println("Invalid tx input", txID, k)
					continue outer
				}
				hash := i.Hash.String()
				unspent, ok := unspentMap[hash]
				if !ok {
					unspent = make(map[uint32]struct {
						string
						int64
					})
					unspentMap[hash] = unspent
				}
				addr2val, ok := unspent[i.Index]
				if !ok {
					unspent[i.Index] = struct {
						string
						int64
					}{}
				} else {
					delete(unspent, i.Index)
					if len(unspent) == 0 {
						delete(unspentMap, txID)
					}

					balance := balanceMap[addr2val.string]
					balance -= addr2val.int64
					if balance <= 0 {
						delete(balanceMap, addr2val.string)
					} else {
						balanceMap[addr2val.string] = balance
					}
				}
			} else {
				//log.Print("coin base")
			}
		}

		unspent, ok := unspentMap[txID]
		if !ok {
			unspent = make(map[uint32]struct {
				string
				int64
			})
		}

		for k, o := range t.Vout {
			addr := btc.NewAddrFromPkScript(o.Script, false)
			if addr != nil {
				balance := balanceMap[addr.String()] + o.Value
				balanceMap[addr.String()] = balance

				key := uint32(k)
				if ok {
					if _, ok := unspent[key]; ok {
						delete(unspent, key)
						continue
					}
				}

				unspent[key] = struct {
					string
					int64
				}{string: addr.String(), int64: o.Value}
			}
		}
		if len(unspent) > 0 {
			unspentMap[txID] = unspent
		}
	}

	b := new(bytes.Buffer)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	for k, v := range balanceMap {
		line := fmt.Sprintln(k, v)
		w.Write([]byte(line))
	}

	w.Close()
	fileName := fmt.Sprintf("/tmp/%v.gz", "balance")
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}

	b = new(bytes.Buffer)
	w, err = gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	bb := new(bytes.Buffer)
	for tx, outputs := range unspentMap {
		bb.WriteString(tx)
		for i, o := range outputs {
			l := fmt.Sprintf(",%v %v %v", i, o.string, o.int64)
			bb.WriteString(l)
		}
		bb.WriteByte('\n')
		w.Write([]byte(bb.Bytes()))
		bb.Reset()
	}

	fileName = fmt.Sprintf("/tmp/%v.gz", "unspent")
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}

	w.Close()

	resCh <- len(balanceMap)
}

func ExportUnspent(wg *sync.WaitGroup, txCh chan *blockchainparser.Transaction, fileNum uint32, magicId blockchainparser.MagicId, datadir string, outDir string) {

	defer wg.Done()

	offset := uint32(8)
	startPos := uint32(0)

	filepath := fmt.Sprintf(datadir+"/blocks/blk%05d.dat", fileNum)
	fi, err := os.Stat(filepath)
	if err != nil {
		log.Fatal(err)
	}
	// get the size
	size := uint32(fi.Size())
	//size /= 5
	// Open file for reading
	blockFile, err := blockchainparser.NewBlockFile(datadir, fileNum)
	if err != nil {
		log.Fatal(err)
	}

	defer blockFile.Close()

	if err != nil {
		log.Fatal(err)
	}

	for startPos < size {
		block, err := blockchainparser.ParseBlockFromFile(blockFile, magicId)
		if err != nil {
			log.Println(err)
			break
		}
		for _, t := range block.Transactions {
			txCh <- &t
		}

		startPos += block.Length + offset
	}
	log.Println(blockFile.FileNum)
}

func GenerateRDF(wg *sync.WaitGroup, fileNum uint32, magicId blockchainparser.MagicId, datadir string, outDir string) {
	defer wg.Done()

	offset := uint32(8)
	startPos := uint32(0)

	filepath := fmt.Sprintf(datadir+"/blocks/blk%05d.dat", fileNum)
	fi, err := os.Stat(filepath)
	if err != nil {
		log.Fatal(err)
	}
	// get the size
	size := uint32(fi.Size())
	//size /= 5
	// Open file for reading
	blockFile, err := blockchainparser.NewBlockFile(datadir, fileNum)
	if err != nil {
		log.Fatal(err)
	}

	defer blockFile.Close()

	if err != nil {
		log.Fatal(err)
	}

	b := new(bytes.Buffer)
	//w := gzip.NewWriter(b)
	w, err := gzip.NewWriterLevel(b, gzip.BestSpeed)
	if err != nil {
		log.Fatal(err)
	}

	for startPos < size {
		block, err := blockchainparser.ParseBlockFromFile(blockFile, magicId)
		if err != nil {
			log.Println(err)
			break
		}
		bh := block.BlockHeader

		blockHash := bh.Hash()

		w.Write([]byte(fmt.Sprintf("<%v> <p> <%v> .\n", blockHash, bh.HashPrev)))
		dt := bh.Timestamp.Format(time.RFC3339)
		w.Write([]byte(fmt.Sprintf("<%v> <ts> \"%v\"^^<xs:dateTime> .\n", blockHash, dt)))

		for _, t := range block.Transactions {
			txID := t.Txid()
			w.Write([]byte(fmt.Sprintf("<%v> <tx> <%v> .\n", blockHash, txID)))
			for _, i := range t.Vin {
				w.Write([]byte(fmt.Sprintf("<%v> <i> <%v> (n=%v) .\n", txID, i.Hash, int32(i.Index))))
			}
			for n, o := range t.Vout {
				if addr := btc.NewAddrFromPkScript(o.Script, false); addr != nil {
					w.Write([]byte(fmt.Sprintf("<%v> <o> <%v> (v=%v, n=%v) .\n",
						txID, addr.String(), float64(o.Value)/1e8, n)))
				}
			}
		}

		startPos += block.Length + offset
	}
	w.Close()
	fileName := fmt.Sprintf("%v/%v.rdf.gz", outDir, fileNum)
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}
	log.Println(fileName)
}
