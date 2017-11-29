package utils

import (
	"testing"
	"log"
)

func TestTTxID_String(t *testing.T) {

	txStr1 := "98cc722af8c8ef9a7d35964431dadee218c7a205870ff0420c0fa6eace55b676"
	txID := makeTxID(txStr1)
	log.Println(len(txID))
	txStr2 := txID.String()
	log.Println(txStr2)
}
