// Copyright (c) 2017-2019 The Bitum developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package commands

import (
	"github.com/bitum-project/politeia/politeiawww/api/cms/v1"
)

// NewInvoiceCmd submits a new invoice.
type InvoiceExchangeRateCmd struct {
	Args struct {
		Month uint `positional-arg-name:"month"` // Invoice Month
		Year  uint `positional-arg-name:"year"`  // Invoice Year
	} `positional-args:"true" optional:"true"`
}

// Execute executes the new invoice command.
func (cmd *InvoiceExchangeRateCmd) Execute(args []string) error {
	month := cmd.Args.Month
	year := cmd.Args.Year

	ier := &v1.InvoiceExchangeRate{
		Month: month,
		Year:  year,
	}

	// Print request details
	err := printJSON(ier)
	if err != nil {
		return err
	}

	// Send request
	ierr, err := client.InvoiceExchangeRate(ier)
	if err != nil {
		return err
	}

	// Print response details
	return printJSON(ierr)
}

const newInvoiceExchangeRateMsg = `invoiceexchangerate [flags]" 

Request an USD/BITUM exchange rate for a given month.

Arguments:
1. month			 (string, required)   Month (MM, 01-12)
2. year				 (string, required)   Year (YYYY)


Result:
{
  "exchangerate": (float64) Calculated rate
}`
