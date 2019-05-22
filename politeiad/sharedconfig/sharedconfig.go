// Copyright (c) 2017-2019 The Bitum developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package sharedconfig

import (
	"github.com/bitum-project/bitumd/bitumutil"
)

const (
	DefaultDataDirname = "data"
)

var (
	DefaultHomeDir = bitumutil.AppDataDir("politeiad", false)
)
