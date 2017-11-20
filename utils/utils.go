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

	for t := range txCh {
		txID := t.Txid().String()
		for _, i := range t.Vin {
			if i.Index != 4294967295 {
				if len(i.Hash) > 256 {
					log.Fatalln("Invalid hash", len(i.Hash))
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

	i := 0
	for k, v := range balanceMap {
		log.Print(k, v)
		if i++; i > 9 {
			break
		}
	}

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

	utxoMap := make(map[string]string)
	balanceMap := make(map[string]int64)

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

		//w.Write([]byte(fmt.Sprintf("<%v> <prev> <%v> .\n", blockHash, bh.HashPrev)))
		w.Write([]byte(fmt.Sprintf("<%v> <p> <%v> .\n", blockHash, bh.HashPrev)))
		//w.Write([]byte(fmt.Sprintf("<%v> <merkle> \"%v\" .\n", blockHash, bh.HashMerkle)))
		//w.Write([]byte(fmt.Sprintf("<%v> <difficulty> \"%v\"^^<xs:int> .\n", blockHash, bh.TargetDifficulty)))
		dt := bh.Timestamp.Format(time.RFC3339)
		//w.Write([]byte(fmt.Sprintf("<%v> <timestamp> \"%v\"^^<xs:dateTime> .\n", blockHash, dt)))
		w.Write([]byte(fmt.Sprintf("<%v> <ts> \"%v\"^^<xs:dateTime> .\n", blockHash, dt)))
		//w.Write([]byte(fmt.Sprintf("<%v> <nonce> \"%v\"^^<xs:int> .\n", blockHash, bh.Nonce)))

		for _, t := range block.Transactions {
			txID := t.Txid()
			//w.Write([]byte(fmt.Sprintf("<%v> <transactions> <%v> .\n", blockHash, txID)))
			w.Write([]byte(fmt.Sprintf("<%v> <tx> <%v> .\n", blockHash, txID)))
			for _, i := range t.Vin {
				outputID := ""
				if i.Index == 4294967295 {
					outputID = fmt.Sprintf("%v.%v", blockHash, 0)
				} else {
					outputID = fmt.Sprintf("%v.%v", i.Hash, i.Index)
					delete(utxoMap, outputID)
				}

				//w.Write([]byte(fmt.Sprintf("<%v> <vin> <%v> .\n", txID, outputID)))
				w.Write([]byte(fmt.Sprintf("<%v> <i> <%v> .\n", txID, outputID)))
			}
			for k, o := range t.Vout {
				outputID := fmt.Sprintf("%v.%v", txID, k)

				addr := btc.NewAddrFromPkScript(o.Script, false)
				if addr != nil {

					balance := balanceMap[addr.String()] + o.Value
					balanceMap[addr.String()] = balance

					//w.Write([]byte(fmt.Sprintf("<%v> <address> <%v> (balance=%v, time=%v) .\n",
					//	outputID, addr.String(), balance, dt)))
					//w.Write([]byte(fmt.Sprintf("<%v> <address> <%v>  .\n", outputID, addr.String())))
					w.Write([]byte(fmt.Sprintf("<%v> <a> <%v>  .\n", outputID, addr.String())))
					//w.Write([]byte(fmt.Sprintf("<%v> <value> \"%v\"^^<xs:int> .\n ", outputID, o.Value)))
					w.Write([]byte(fmt.Sprintf("<%v> <v> \"%v\"^^<xs:int> .\n ", outputID, o.Value)))

					//w.Write([]byte(fmt.Sprintf("<%v> <vout> <%v> .\n", txID, outputID)))
					w.Write([]byte(fmt.Sprintf("<%v> <o> <%v> .\n", txID, outputID)))
				} else {
					//log.Printf("cannot resolve address from script:%v", o.Script)
				}
			}
		}

		//log.Print(block.Hash())
		startPos += block.Length + offset
	}
	w.Close()
	fileName := fmt.Sprintf("%v/%v.rdf.gz", outDir, fileNum)
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0666); err != nil {
		log.Fatal(err)
	}
	log.Println(fileName)
}
