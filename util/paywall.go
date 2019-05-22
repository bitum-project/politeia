// Copyright (c) 2017-2019 The Bitum developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bitum-project/bitumd/chaincfg"
	"github.com/bitum-project/bitumd/bitumutil"
	"github.com/bitum-project/bitumd/hdkeychain"
	"github.com/bitum-project/bitumwallet/wallet/udb"
)

const (
	bitumdataMainnet = "https://explorer.bitum.io/api"
	bitumdataTestnet = "https://testnet.bitum.io/api"
	insightMainnet = "https://mainnet.bitum.io/insight/api"
	insightTestnet = "https://testnet.bitum.io/insight/api"

	bitumdataTimeout = 3 * time.Second // Bitumdata request timeout
	faucetTimeout  = 5 * time.Second // Testnet faucet request timeout
)

// FaucetResponse represents the expected JSON response from the testnet faucet.
type FaucetResponse struct {
	Txid  string
	Error string
}

// PaywallGatewayNewOrderResponse respresents the expected JSON response to a
// PaywallGatewayNewOrder command.
type PaywallGatewayNewOrderResponse struct {
	Error         string
	OrderID       string
	PaywallAmount uint64
}

// BEPrimaryTransaction is an object representing a transaction; it's
// part of the data returned from the URL for the primary block explorer
// when fetching the transactions for an address.
type BEPrimaryTransaction struct {
	TxId          string                     `json:"txid"`          // Transaction id
	Vout          []BEPrimaryTransactionVout `json:"vout"`          // Transaction outputs
	Confirmations uint64                     `json:"confirmations"` // Number of confirmations
	Timestamp     int64                      `json:"time"`          // Transaction timestamp
}

// BEPrimaryTransactionVout holds the transaction amount information.
type BEPrimaryTransactionVout struct {
	Amount       json.Number                      `json:"value"`        // Transaction amount (in BITUM)
	ScriptPubkey BEPrimaryTransactionScriptPubkey `json:"scriptPubkey"` // Transaction script info
}

// BEPrimaryTransactionScriptPubkey holds the script info for a
// transaction.
type BEPrimaryTransactionScriptPubkey struct {
	Addresses []string `json:"addresses"` // Array of transaction input addresses
}

// BEBackupTransaction is an object representing a transaction; it's
// part of the data returned from the URL for the backup block explorer
// when fetching the transactions for an address.
type BEBackupTransaction struct {
	Address       string      `json:"address"`       // Transaction address
	TxId          string      `json:"txid"`          // Transaction id
	Amount        json.Number `json:"amount"`        // Transaction amount (in BITUM)
	Confirmations uint64      `json:"confirmations"` // Number of confirmations
	Timestamp     int64       `json:"ts"`            // Transaction timestamp
}

// TxDetails is an object representing a transaction that is used to
// standardize the different responses from bitumdata and insight.
type TxDetails struct {
	Address       string // Transaction address
	TxID          string // Transacion ID
	Amount        uint64 // Transaction amount (in atoms)
	Timestamp     int64  // Transaction timestamp
	Confirmations uint64 // Number of confirmations
}

var (
	// ErrCannotVerifyPayment is emitted when a transaction cannot be verified
	// because it cannot reach either of the block explorer servers.
	ErrCannotVerifyPayment = errors.New("cannot verify payment at this time")
)

func makeRequest(url string, timeout time.Duration) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %v", err)
	}

	client := &http.Client{
		Timeout: timeout * time.Second,
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("bitumdata error: %v %v %v",
				response.StatusCode, url, err)
		}
		return nil, fmt.Errorf("bitumdata error: %v %v %s",
			response.StatusCode, url, body)
	}

	return ioutil.ReadAll(response.Body)
}

// BitumStringToAmount converts a BITUM amount as a string into a uint64
// representing atoms. Supported input variations: "1", ".1", "0.1"
func BitumStringToAmount(bitumstr string) (uint64, error) {
	match, err := regexp.MatchString("(\\d*\\.)*\\d+", bitumstr)
	if err != nil {
		return 0, err
	}
	if !match {
		return 0, fmt.Errorf("invalid BITUM amount: %v", bitumstr)
	}

	var bitumsplit []string
	if strings.Contains(bitumstr, ".") {
		bitumsplit = strings.Split(bitumstr, ".")
		if len(bitumsplit[0]) == 0 {
			bitumsplit[0] = "0"
		}
	} else {
		bitumsplit = []string{bitumstr, "0"}
	}

	whole, err := strconv.ParseUint(bitumsplit[0], 10, 64)
	if err != nil {
		return 0, err
	}

	bitumsplit[1] += "00000000"
	fraction, err := strconv.ParseUint(bitumsplit[1][0:8], 10, 64)
	if err != nil {
		return 0, err
	}

	return ((whole * 1e8) + fraction), nil
}

func blockExplorerURLForAddress(address string, netParams *chaincfg.Params) (string, string, error) {
	var (
		bitumdata string
		insight string
	)

	switch netParams {
	case &chaincfg.MainNetParams:
		bitumdata = bitumdataMainnet + "/address/" + address
		insight = insightMainnet + "/addr/" + address
	case &chaincfg.TestNetParams:
		bitumdata = bitumdataTestnet + "/address/" + address
		insight = insightTestnet + "/addr/" + address
	default:
		return "", "", fmt.Errorf("unsupported network %v",
			getNetworkName(netParams))
	}

	return bitumdata, insight, nil
}

func fetchTxWithPrimaryBE(url string, address string, minimumAmount uint64, txnotbefore int64, minConfirmationsRequired uint64) (string, uint64, error) {
	responseBody, err := makeRequest(url, bitumdataTimeout)
	if err != nil {
		return "", 0, err
	}

	transactions := make([]BEPrimaryTransaction, 0)
	err = json.Unmarshal(responseBody, &transactions)
	if err != nil {
		return "", 0, err
	}

	for _, v := range transactions {
		if v.Timestamp < txnotbefore {
			continue
		}
		if v.Confirmations < minConfirmationsRequired {
			continue
		}

		for _, vout := range v.Vout {
			amount, err := BitumStringToAmount(vout.Amount.String())
			if err != nil {
				return "", 0, err
			}

			if amount < minimumAmount {
				continue
			}

			for _, addr := range vout.ScriptPubkey.Addresses {
				if address == addr {
					return v.TxId, amount, nil
				}
			}
		}
	}

	return "", 0, nil
}

func fetchTxWithBackupBE(url string, address string, minimumAmount uint64, txnotbefore int64, minConfirmationsRequired uint64) (string, uint64, error) {
	responseBody, err := makeRequest(url, bitumdataTimeout)
	if err != nil {
		return "", 0, err
	}

	transactions := make([]BEBackupTransaction, 0)
	err = json.Unmarshal(responseBody, &transactions)
	if err != nil {
		return "", 0, err
	}

	for _, v := range transactions {
		if v.Timestamp < txnotbefore {
			continue
		}
		if v.Confirmations < minConfirmationsRequired {
			continue
		}

		amount, err := BitumStringToAmount(v.Amount.String())
		if err != nil {
			return "", 0, err
		}
		if amount < minimumAmount {
			continue
		}

		return v.TxId, amount, nil
	}

	return "", 0, nil
}

func getNetworkName(params *chaincfg.Params) string {
	if strings.HasPrefix(params.Name, "testnet") {
		return "testnet"
	}
	return params.Name
}

// DerivePaywallAddress derives a paywall address using the provided xpub and
// index.
func DerivePaywallAddress(params *chaincfg.Params, xpub string, index uint32) (string, error) {
	// Parse the extended public key.
	acctKey, err := hdkeychain.NewKeyFromString(xpub)
	if err != nil {
		return "", err
	}

	// Derive the appropriate branch key.
	branchKey, err := acctKey.Child(udb.ExternalBranch)
	if err != nil {
		return "", err
	}

	key, err := branchKey.Child(index)
	if err != nil {
		return "", err
	}

	addr, err := key.Address(params)
	if err != nil {
		return "", err
	}

	return addr.EncodeAddress(), nil
}

// PayWithTestnetFaucet makes a request to the testnet faucet.
func PayWithTestnetFaucet(faucetURL string, address string, amount uint64, overridetoken string) (string, error) {
	bitumaddress, err := bitumutil.DecodeAddress(address)
	if err != nil {
		return "", fmt.Errorf("address is invalid: %v", err)
	}

	if !bitumaddress.IsForNet(&chaincfg.TestNetParams) {
		return "", fmt.Errorf("faucet only supports testnet")
	}

	bitumamount := strconv.FormatFloat(bitumutil.Amount(amount).ToCoin(),
		'f', -1, 32)

	// build request
	form := url.Values{}
	form.Add("address", address)
	form.Add("amount", bitumamount)
	form.Add("overridetoken", overridetoken)

	req, err := http.NewRequest("POST", faucetURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.PostForm = form
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	// limit the time we take
	ctx, cancel := context.WithTimeout(context.Background(),
		faucetTimeout)
	// it is good practice to use the cancellation function even with a timeout
	defer cancel()
	req = req.WithContext(ctx)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("testnet faucet error: %v %v %v",
				resp.StatusCode, faucetURL, err)
		}
		return "", fmt.Errorf("testnet faucet error: %v %v %s",
			resp.StatusCode, faucetURL, body)
	}

	if resp == nil {
		return "", errors.New("unknown error")
	}

	jsonReply := resp.Header.Get("X-Json-Reply")
	if jsonReply == "" {
		return "", fmt.Errorf("bad reply from %v", faucetURL)
	}

	fr := &FaucetResponse{}
	err = json.Unmarshal([]byte(jsonReply), fr)
	if err != nil {
		return "", fmt.Errorf("unable to process reply: '%v': %v", jsonReply,
			err)
	}

	if fr.Error != "" {
		return "", errors.New(fr.Error)
	}

	return fr.Txid, nil
}

// FetchTxWithBlockExplorers uses public block explorers to look for a
// transaction for the given address that equals or exceeds the given amount,
// occurs after the txnotbefore time and has the minimum number of confirmations.
func FetchTxWithBlockExplorers(address string, amount uint64, txnotbefore int64, minConfirmations uint64) (string, uint64, error) {
	// pre-validate that the passed address, amount, and tx are at least
	// somewhat valid before querying the explorers
	addr, err := bitumutil.DecodeAddress(address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address %v: %v", addr, err)
	}

	bitumdataURL, insightURL, err := blockExplorerURLForAddress(address,
		addr.Net())
	if err != nil {
		return "", 0, err
	}
	primaryURL := bitumdataURL + "/raw"
	backupURL := insightURL + "/utxo?noCache=1"

	// Try the primary (bitumdata) first.
	txID, amount, err := fetchTxWithPrimaryBE(primaryURL, address, amount,
		txnotbefore, minConfirmations)
	if err != nil {
		log.Printf("failed to fetch from bitumdata: %v", err)
	} else {
		return txID, amount, nil
	}

	// Try the backup (insight).
	txID, amount, err = fetchTxWithBackupBE(backupURL, address, amount,
		txnotbefore, minConfirmations)
	if err != nil {
		log.Printf("failed to fetch from insight: %v", err)
		return "", 0, ErrCannotVerifyPayment
	}

	return txID, amount, nil
}

func fetchTxsWithPrimaryBE(url string) ([]BEPrimaryTransaction, error) {
	responseBody, err := makeRequest(url, bitumdataTimeout)
	if err != nil {
		return nil, err
	}

	transactions := make([]BEPrimaryTransaction, 0)
	err = json.Unmarshal(responseBody, &transactions)
	if err != nil {
		return nil, fmt.Errorf("Unmarshal []BEPrimaryTransaction: %v", err)
	}

	return transactions, nil
}

func fetchTxsWithBackupBE(url string) ([]BEBackupTransaction, error) {
	responseBody, err := makeRequest(url, bitumdataTimeout)
	if err != nil {
		return nil, err
	}

	transactions := make([]BEBackupTransaction, 0)
	err = json.Unmarshal(responseBody, &transactions)
	if err != nil {
		return nil, fmt.Errorf("Unmarshal []BEBackupTransaction: %v", err)
	}

	return transactions, nil
}

func convertBEPrimaryTransactionToTxDetails(address string, tx BEPrimaryTransaction) (*TxDetails, error) {
	var amount uint64
	for _, vout := range tx.Vout {
		amt, err := BitumStringToAmount(vout.Amount.String())
		if err != nil {
			return nil, err
		}

		for _, addr := range vout.ScriptPubkey.Addresses {
			if address == addr {
				amount += amt
			}
		}
	}

	return &TxDetails{
		Address:       address,
		TxID:          tx.TxId,
		Amount:        amount,
		Confirmations: tx.Confirmations,
		Timestamp:     tx.Timestamp,
	}, nil
}

func convertBEBackupTransactionToTxDetails(tx BEBackupTransaction) (*TxDetails, error) {
	amount, err := BitumStringToAmount(tx.Amount.String())
	if err != nil {
		return nil, err
	}

	return &TxDetails{
		Address:       tx.Address,
		TxID:          tx.TxId,
		Amount:        amount,
		Confirmations: tx.Confirmations,
		Timestamp:     tx.Timestamp,
	}, nil
}

// FetchTxsForAddress fetches the transactions that have been sent to the
// provided wallet address from the bitumdata block explorer. If the bitumdata
// request fails the insight block explorer is tried.
func FetchTxsForAddress(address string) ([]TxDetails, error) {
	// Get block explorer URLs
	addr, err := bitumutil.DecodeAddress(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address %v: %v", addr, err)
	}
	bitumdataURL, insightURL, err := blockExplorerURLForAddress(address,
		addr.Net())
	if err != nil {
		return nil, err
	}
	primaryURL := bitumdataURL + "/raw"
	backupURL := insightURL + "/utxo?noCache=1"

	// Try the primary (bitumdata)
	primaryTxs, err := fetchTxsWithPrimaryBE(primaryURL)
	if err != nil {
		log.Printf("failed to fetch from bitumdata: %v", err)
	} else {
		txs := make([]TxDetails, 0, len(primaryTxs))
		for _, tx := range primaryTxs {
			txDetail, err := convertBEPrimaryTransactionToTxDetails(address, tx)
			if err != nil {
				return nil, fmt.Errorf("convertBEPrimaryTransactionToTxDetails: %v",
					tx.TxId)
			}
			txs = append(txs, *txDetail)
		}

		return txs, nil
	}

	// Try the backup (insight)
	backupTxs, err := fetchTxsWithBackupBE(backupURL)
	if err != nil {
		log.Printf("failed to fetch from insight: %v", err)
		return nil, ErrCannotVerifyPayment
	}

	txs := make([]TxDetails, 0, len(backupTxs))
	for _, tx := range backupTxs {
		txDetail, err := convertBEBackupTransactionToTxDetails(tx)
		if err != nil {
			return nil, fmt.Errorf("convertBEBackupTransactionToTxDetails: %v",
				tx.TxId)
		}
		txs = append(txs, *txDetail)
	}

	return txs, nil
}

// FetchTxsForAddressNotBefore fetches all transactions for a wallet address
// that occurred after the passed in notBefore timestamp.
func FetchTxsForAddressNotBefore(address string, notBefore int64) ([]TxDetails, error) {
	// Get block explorer URL
	addr, err := bitumutil.DecodeAddress(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address %v: %v", addr, err)
	}
	bitumdataURL, _, err := blockExplorerURLForAddress(address, addr.Net())
	if err != nil {
		return nil, err
	}

	// Fetch all txs for the passed in wallet address
	// that were sent after the notBefore timestamp
	var (
		targetTxs []TxDetails
		count     = 10
		skip      = 0
		done      = false
	)
	for !done {
		// Fetch a page of user payment txs
		url := bitumdataURL + "/count/" + strconv.Itoa(count) +
			"/skip/" + strconv.Itoa(skip) + "/raw"
		bitumdataTxs, err := fetchTxsWithPrimaryBE(url)
		if err != nil {
			return nil, fmt.Errorf("fetchBitumdataAddress: %v", err)
		}

		// Convert transactions to TxDetails
		txs := make([]TxDetails, len(bitumdataTxs))
		for _, tx := range bitumdataTxs {
			txDetails, err := convertBEPrimaryTransactionToTxDetails(address, tx)
			if err != nil {
				return nil, fmt.Errorf("convertBEPrimaryTransactionToTxDetails: %v",
					tx.TxId)
			}
			txs = append(txs, *txDetails)
		}

		// Sanity check. Txs should already be sorted
		// in reverse chronological order.
		sort.SliceStable(txs, func(i, j int) bool {
			return txs[i].Timestamp > txs[j].Timestamp
		})

		// Verify txs are within notBefore limit
		for _, tx := range txs {
			if tx.Timestamp > notBefore {
				targetTxs = append(targetTxs, tx)
			} else {
				// We have reached the notBefore
				// limit; stop requesting txs
				done = true
				break
			}
		}

		skip += count
	}

	return targetTxs, nil
}
