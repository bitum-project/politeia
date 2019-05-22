// Copyright (c) 2017-2019 The Bitum developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package sharedconfig

import (
	"path/filepath"

	"github.com/bitum-project/bitumd/bitumutil"
)

const (
	DefaultConfigFilename = "politeiawww.conf"
	DefaultDataDirname    = "data"
)

var (
	// DefaultHomeDir points to politeiawww's home directory for configuration and data.
	DefaultHomeDir = bitumutil.AppDataDir("politeiawww", false)

	// DefaultConfigFile points to politeiawww's default config file.
	DefaultConfigFile = filepath.Join(DefaultHomeDir, DefaultConfigFilename)

	// DefaultDataDir points to politeiawww's default data directory.
	DefaultDataDir = filepath.Join(DefaultHomeDir, DefaultDataDirname)
)
