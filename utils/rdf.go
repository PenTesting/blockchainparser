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
	"regexp"
	"strconv"
	"runtime"
)

func ExportRDF(datadir string, magicId blockchainparser.MagicId, outDir string) {

	log.Print("start")

	wg := new(sync.WaitGroup)

	i := 0
	cpuNum := runtime.NumCPU()
	if files, err := ioutil.ReadDir(datadir + "/blocks/"); err == nil {
		for _, f := range files {
			r, err := regexp.Compile("blk(\\d+)\\.dat") // Do we have an 'N' or 'index' at the beginning?
			if err != nil {
				log.Println(err)
				break
			}
			if matches := r.FindStringSubmatch(f.Name()); len(matches) == 2 {
				if fileNum, err := strconv.Atoi(matches[1]); err == nil {
					wg.Add(1)
					go generateRDF(wg, uint32(fileNum), magicId, datadir, outDir)
					if i++; i%cpuNum == 0 {
						wg.Wait()
					}
				}
			}
		}
	}
	wg.Wait()
}

func generateRDF(wg *sync.WaitGroup, fileNum uint32, magicId blockchainparser.MagicId, datadir string, outDir string) {
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

			inMap := make(map[string]string)
			for i, in := range t.Vin {
				tx := in.Hash.String()
				if last, ok := inMap[tx]; ok {
					inMap[tx] = fmt.Sprint(last, " ", int32(in.Index))
					inMap[tx] = fmt.Sprintf("%v,\"%v\"=%v", last, int32(in.Index), i)
					//log.Println("in", txID, tx, inMap[tx])
				} else {
					inMap[tx] = fmt.Sprintf("\"%v\"=%v", int32(in.Index), i)
				}
			}
			for tx, index := range inMap {
				w.Write([]byte(fmt.Sprintf("<%v> <i> <%v> (%v) .\n", txID, tx, index)))
			}

			outMap := make(map[string]string)
			for i, out := range t.Vout {
				if addr := btc.NewAddrFromPkScript(out.Script, false); addr != nil {
					addrStr := addr.String()
					val := float64(out.Value) / 1e8
					if last, ok := outMap[addrStr]; ok {
						outMap[addrStr] = fmt.Sprintf("%v,\"%v\"=%v", last, i, val)
						//log.Println("out", txID, addrStr, outMap[addrStr])
					} else {
						outMap[addrStr] = fmt.Sprintf("\"%v\"=%v", i, val)
					}
				}
			}

			for addr, out := range outMap {
				w.Write([]byte(fmt.Sprintf("<%v> <o> <%v> (%v) .\n", txID, addr, out)))
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
