package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The Parser interface
type Parser interface {
	// last parsed block
	GetCurrentBlock() int

	// add address to observer
	Subscribe(address string) bool

	// list of inbound or outbound transactions for an address
	GetTransactions(address string) []*Transaction
}

type StorageProvider interface {
	AddTargetAddress(address string) bool
	SaveTransactions(block int, txs []*Transaction)
	GetTransactions(address string) []*Transaction
	GetCurrentBlock() int
}

type Transaction struct {
	BlockHash            string
	BlockNumber          string
	From                 string
	Gas                  string
	GasPrice             string
	MaxFeePerGas         string
	MaxPriorityFeePerGas string
	Hash                 string
	Input                string
	Nonce                string
	To                   string
	TransactionIndex     string
	Value                string
	Type                 string
	AccessList           []interface{}
	ChainId              string
	V, R, S              string
	YParity              string
}

// The mem storage
type MemStorage struct {
	currentBlock int
	txs          map[string][]*Transaction
	sync.RWMutex
}

func NewMemStorage() *MemStorage {
	return &MemStorage{txs: make(map[string][]*Transaction)}
}

func (ms *MemStorage) GetCurrentBlock() int {
	ms.RLock()
	defer ms.RUnlock()
	return ms.currentBlock
}

func (ms *MemStorage) AddTargetAddress(address string) bool {
	ms.Lock()
	defer ms.Unlock()
	address = strings.ToLower(address)
	_, ok := ms.txs[address]
	if !ok {
		ms.txs[strings.ToLower(address)] = nil
		return true
	} else {
		return false
	}
}

func (ms *MemStorage) SaveTransactions(block int, txs []*Transaction) {
	ms.Lock()
	defer ms.Unlock()
	for _, tx := range txs {
		from, to := strings.ToLower(tx.From), strings.ToLower(tx.To)
		if _, ok := ms.txs[from]; ok {
			fmt.Println("New outgoing transaction", "hash", tx.Hash)
			ms.txs[from] = append(ms.txs[from], tx)
		}
		if _, ok := ms.txs[to]; ok {
			fmt.Println("New incoming transaction", "hash", tx.Hash)
			ms.txs[to] = append(ms.txs[to], tx)
		}
	}
	ms.currentBlock = block
}

func (ms *MemStorage) GetTransactions(address string) []*Transaction {
	ms.RLock()
	defer ms.RUnlock()
	return ms.txs[strings.ToLower(address)]
}

// The IParser implementation
type EthParser struct {
	url     string
	storage StorageProvider
}

func NewEthParser(url string) *EthParser {
	parser := &EthParser{
		url:     url,
		storage: NewMemStorage(),
	}
	return parser
}

// last parsed block
func (p *EthParser) GetCurrentBlock() int {
	return p.storage.GetCurrentBlock()
}

// add address to observer
func (p *EthParser) Subscribe(address string) bool {
	return p.storage.AddTargetAddress(address)
}

// list of inbound or outbound transactions for an address
func (p *EthParser) GetTransactions(address string) []*Transaction {
	return p.storage.GetTransactions(address)
}

// Start the parser subscription
func (p *EthParser) Start() {
	var (
		err          error
		txs          []*Transaction
		latestBlock  int
		currentBlock = p.storage.GetCurrentBlock()
	)
LOOP:
	for {
		if err != nil {
			// backoff errors like ratelimit
			fmt.Printf("Last RPC call error %v, will backoff one second. \n", err)
			time.Sleep(time.Second)
		}
		for currentBlock < latestBlock {
			txs, err = p.FetchBlock(currentBlock + 1)
			if err != nil {
				continue LOOP
			}
			p.storage.SaveTransactions(currentBlock+1, txs)
			currentBlock++
			fmt.Println("Parsed block", currentBlock, "transactions count", len(txs))
		}
		latestBlock, err = p.GetLatestBlockNumber()
	}
}

func postJsonFor(url string, payload, result interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	// req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if respBody, err := io.ReadAll(resp.Body); err != nil {
		return err
	} else {
		return json.Unmarshal(respBody, &result)
	}
}

func (p *EthParser) FetchBlock(block int) (txs []*Transaction, err error) {
	params := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{fmt.Sprintf("0x%x", block), true},
	}
	var result struct {
		Code    int
		Jsonrpc string
		Result struct {
			Transactions []*Transaction
		}
	}
	err = postJsonFor(p.url, params, &result)
	if err == nil {
		if result.Code != 0 {
			err = fmt.Errorf("failed rpc request, code %d", result.Code)
		} else {
			txs = result.Result.Transactions
		}
	}
	return
}

func (p *EthParser) GetLatestBlockNumber() (block int, err error) {
	params := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "eth_blockNumber",
		"params":  []interface{}{},
	}
	var result struct {
		Code    int
		Jsonrpc string
		Result  string
	}
	err = postJsonFor(p.url, params, &result)
	if err == nil {
		if result.Code != 0 {
			err = fmt.Errorf("failed rpc request, code %d", result.Code)
		} else {
			var blockNumber int64
			if blockNumber, err = strconv.ParseInt(result.Result, 0, 0); err == nil {
				block = int(blockNumber)
			}
		}
	}
	return
}

type HttpServer struct {
	parser Parser
}

func writeAsJson(w http.ResponseWriter, v interface{}) {
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("failed to marshal value, err %v", err))
	}
	w.Write(bytes)
}

func (s *HttpServer) HandleGetCurrentBlock(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	writeAsJson(w, map[string]interface{}{
		"currentBlock": s.parser.GetCurrentBlock(),
	})
}

func (s *HttpServer) HandleSubscribe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	address := r.PathValue("address")
	writeAsJson(w, map[string]interface{}{
		"address":    address,
		"success": s.parser.Subscribe(address),
	})
}

func (s *HttpServer) HandleGetTransactions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	address := r.PathValue("address")
	writeAsJson(w, map[string]interface{}{
		"address":      address,
		"transactions": s.parser.GetTransactions(address),
	})
}

func NewHttpServer(parser Parser) *HttpServer {
	return &HttpServer{parser: parser}
}

func (s *HttpServer) Serve(addr string) {
	http.HandleFunc("/GetCurrentBlock", s.HandleGetCurrentBlock)
	http.HandleFunc("/Subscribe/{address}", s.HandleSubscribe)
	http.HandleFunc("/GetTransactions/{address}", s.HandleGetTransactions)

	err := http.ListenAndServe(addr, nil)
	if err != nil {
		panic(fmt.Errorf("failed to serve http, err %v", err))
	}
}

func main() {
	// Create the parser
	parser := NewEthParser("https://cloudflare-eth.com")

	// Setup for test:
	//	parser.Subscribe("0x23a50Cc8fa9B1B57732010AA24F592Cfe8aaB47A")
	//	parser.storage.SaveTransactions(10000000, nil)
	

	// Expose as http server
	server := NewHttpServer(parser)
	go server.Serve("localhost:8888")

	// Start the parser
	parser.Start()
}

/*
README:

// NOTE:
//  - Requirement: golang 1.22

// Run
go run .

// GetCurrentBlock
curl localhost:8888/GetCurrentBlock

// Subscribe
curl localhost:8888/Subscribe/0x23a50Cc8fa9B1B57732010AA24F592Cfe8aaB47A


// GetTransactions
curl localhost:8888/GetTransactions/0x23a50Cc8fa9B1B57732010AA24F592Cfe8aaB47A

*/
