// Copyright (c) 2017-2019 The Bitum developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package cockroachdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/url"
	"sync"

	"github.com/bitum-project/politeia/politeiawww/user"
	"github.com/bitum-project/politeia/util"
	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/marcopeereboom/sbox"
)

const (
	databaseID             = "users"
	databaseVersion uint32 = 1

	// Database table names
	tableKeyValue   = "key_value"
	tableUsers      = "users"
	tableIdentities = "identities"

	// Database user (read/write access)
	userPoliteiawww = "politeiawww"

	// Key-value store keys
	keyVersion             = "version"
	keyPaywallAddressIndex = "paywalladdressindex"
)

// cockroachdb implements the user database interface.
type cockroachdb struct {
	sync.RWMutex

	shutdown      bool      // Backend is shutdown
	encryptionKey *[32]byte // Data at rest encryption key
	userDB        *gorm.DB  // Database context
}

// encrypt encrypts the provided data with the cockroachdb encryption key. The
// encrypted blob is prefixed with an sbox header which encodes the provided
// version. The read lock is taken despite the encryption key being a static
// value because the encryption key is zeroed out on shutdown, which causes
// race conditions to be reported when the golang race detector is used.
//
// This function must be called without the lock held.
func (c *cockroachdb) encrypt(version uint32, b []byte) ([]byte, error) {
	c.RLock()
	defer c.RUnlock()

	return sbox.Encrypt(version, c.encryptionKey, b)
}

// decrypt decrypts the provided packed blob using the cockroachdb encryption
// key. The read lock is taken despite the encryption key being a static value
// because the encryption key is zeroed out on shutdown, which causes race
// conditions to be reported when the golang race detector is used.
//
// This function must be called without the lock held.
func (c *cockroachdb) decrypt(b []byte) ([]byte, uint32, error) {
	c.RLock()
	defer c.RUnlock()

	return sbox.Decrypt(c.encryptionKey, b)
}

// setPaywallAddressIndex updates the paywall address index record in the
// key-value store.
//
// This function can be called using a transaction when necessary.
func setPaywallAddressIndex(db *gorm.DB, index uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, index)
	kv := KeyValue{
		Key:   keyPaywallAddressIndex,
		Value: b,
	}
	return db.Save(&kv).Error
}

// SetPaywallAddressIndex updates the paywall address index record in the
// key-value database table.
func (c *cockroachdb) SetPaywallAddressIndex(index uint64) error {
	log.Tracef("SetPaywallAddressIndex: %v", index)

	c.RLock()
	shutdown := c.shutdown
	c.RUnlock()

	if shutdown {
		return user.ErrShutdown
	}

	return setPaywallAddressIndex(c.userDB, index)
}

// userNew creates a new user the database.  The userID and paywall address
// index are set before the user record is inserted into the database.
//
// This function must be called using a transaction.
func (c *cockroachdb) userNew(tx *gorm.DB, u user.User) error {
	// Set user paywall address index
	var index uint64
	kv := KeyValue{
		Key: keyPaywallAddressIndex,
	}
	err := tx.Find(&kv).Error
	if err != nil {
		if err != gorm.ErrRecordNotFound {
			return fmt.Errorf("find paywall index: %v", err)
		}
	} else {
		index = binary.LittleEndian.Uint64(kv.Value) + 1
	}

	u.PaywallAddressIndex = index

	// Set user ID
	u.ID = uuid.New()

	// Create user record
	ub, err := user.EncodeUser(u)
	if err != nil {
		return err
	}

	eb, err := c.encrypt(user.VersionUser, ub)
	if err != nil {
		return err
	}

	ur := convertUserFromUser(u, eb)
	err = tx.Create(&ur).Error
	if err != nil {
		return fmt.Errorf("create user: %v", err)
	}

	// Update paywall address index
	err = setPaywallAddressIndex(tx, index)
	if err != nil {
		return fmt.Errorf("set paywall index: %v", err)
	}

	return nil
}

// UserNew creates a new user record in the database.
func (c *cockroachdb) UserNew(u user.User) error {
	log.Tracef("UserNew: %v", u.Username)

	c.RLock()
	shutdown := c.shutdown
	c.RUnlock()

	if shutdown {
		return user.ErrShutdown
	}

	// Create new user with a transaction
	tx := c.userDB.Begin()
	err := c.userNew(tx, u)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

// UserGetByUsername returns a user record given its username, if found in the
// database.
func (c *cockroachdb) UserGetByUsername(username string) (*user.User, error) {
	log.Tracef("UserGetByUsername: %v", username)

	c.RLock()
	shutdown := c.shutdown
	c.RUnlock()

	if shutdown {
		return nil, user.ErrShutdown
	}

	var u User
	err := c.userDB.
		Where("username = ?", username).
		Find(&u).
		Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			err = user.ErrUserNotFound
		}
		return nil, err
	}

	b, _, err := c.decrypt(u.Blob)
	if err != nil {
		return nil, err
	}

	usr, err := user.DecodeUser(b)
	if err != nil {
		return nil, err
	}

	return usr, nil
}

// UserGetByUsername returns a user record given its UUID, if found in the
// database.
func (c *cockroachdb) UserGetById(id uuid.UUID) (*user.User, error) {
	log.Tracef("UserGetById: %v", id)

	c.RLock()
	shutdown := c.shutdown
	c.RUnlock()

	if shutdown {
		return nil, user.ErrShutdown
	}

	var u User
	err := c.userDB.
		Where("id = ?", id).
		Find(&u).
		Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			err = user.ErrUserNotFound
		}
		return nil, err
	}

	b, _, err := c.decrypt(u.Blob)
	if err != nil {
		return nil, err
	}

	usr, err := user.DecodeUser(b)
	if err != nil {
		return nil, err
	}

	return usr, nil
}

// UserUpdate updates an existing user record in the database.
func (c *cockroachdb) UserUpdate(u user.User) error {
	log.Tracef("UserUpdate: %v", u.Username)

	c.RLock()
	shutdown := c.shutdown
	c.RUnlock()

	if shutdown {
		return user.ErrShutdown
	}

	b, err := user.EncodeUser(u)
	if err != nil {
		return err
	}

	eb, err := c.encrypt(user.VersionUser, b)
	if err != nil {
		return err
	}

	ur := convertUserFromUser(u, eb)
	return c.userDB.Save(ur).Error
}

// AllUsers iterates over every user in the database, invoking the given
// callback function on each user.
func (c *cockroachdb) AllUsers(callback func(u *user.User)) error {
	log.Tracef("AllUsers")

	c.RLock()
	shutdown := c.shutdown
	c.RUnlock()

	if shutdown {
		return user.ErrShutdown
	}

	// Lookup all users
	var users []User
	err := c.userDB.Find(&users).Error
	if err != nil {
		return err
	}

	// Invoke callback on each user
	for _, v := range users {
		b, _, err := c.decrypt(v.Blob)
		if err != nil {
			return err
		}

		u, err := user.DecodeUser(b)
		if err != nil {
			return err
		}

		callback(u)
	}

	return nil
}

// rotateKeys rotates the existing database encryption key with the given new
// key.
//
// This function must be called using a transaction.
func rotateKeys(tx *gorm.DB, oldKey *[32]byte, newKey *[32]byte) error {
	// Lookup all users
	var users []User
	err := tx.Find(&users).Error
	if err != nil {
		return err
	}

	// Rotate keys
	for _, v := range users {
		b, _, err := sbox.Decrypt(oldKey, v.Blob)
		if err != nil {
			return fmt.Errorf("decrypt user '%v': %v",
				v.ID, err)
		}

		eb, err := sbox.Encrypt(user.VersionUser, newKey, b)
		if err != nil {
			return fmt.Errorf("encrypt user '%v': %v",
				v.ID, err)
		}

		v.Blob = eb
		err = tx.Save(&v).Error
		if err != nil {
			return fmt.Errorf("save user '%v': %v",
				v.ID, err)
		}
	}

	return nil
}

// RotateKeys rotates the existing database encryption key with the given new
// key.
func (c *cockroachdb) RotateKeys(newKeyPath string) error {
	log.Tracef("RotateKeys: %v", newKeyPath)

	c.Lock()
	defer c.Unlock()

	if c.shutdown {
		return user.ErrShutdown
	}

	// Load and validate new encryption key
	newKey, err := loadEncryptionKey(newKeyPath)
	if err != nil {
		return fmt.Errorf("load encryption key '%v': %v",
			newKeyPath, err)
	}

	if bytes.Equal(newKey[:], c.encryptionKey[:]) {
		return fmt.Errorf("keys are the same")
	}

	log.Infof("Rotating encryption keys")

	// Rotate keys using a transaction
	tx := c.userDB.Begin()
	err = rotateKeys(tx, c.encryptionKey, newKey)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit().Error
	if err != nil {
		return fmt.Errorf("commit tx: %v", err)
	}

	// Update context
	c.encryptionKey = newKey

	return nil
}

// InsertUser inserts a user record into the database.
func (c *cockroachdb) InsertUser(u user.User) error {
	log.Tracef("InsertUser: %v", u.ID)

	c.RLock()
	shutdown := c.shutdown
	c.RUnlock()

	if shutdown {
		return user.ErrShutdown
	}

	ub, err := user.EncodeUser(u)
	if err != nil {
		return err
	}

	eb, err := c.encrypt(user.VersionUser, ub)
	if err != nil {
		return err
	}

	ur := convertUserFromUser(u, eb)
	return c.userDB.Create(&ur).Error
}

// Close shuts down the database.  All interface functions must return with
// errShutdown if the backend is shutting down.
func (c *cockroachdb) Close() error {
	log.Tracef("Close")

	c.Lock()
	defer c.Unlock()

	// Zero out encryption key
	util.Zero(c.encryptionKey[:])
	c.encryptionKey = nil

	c.shutdown = true
	return c.userDB.Close()
}

func (c *cockroachdb) createTables(tx *gorm.DB) error {
	if !tx.HasTable(tableKeyValue) {
		err := tx.CreateTable(&KeyValue{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableUsers) {
		err := tx.CreateTable(&User{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableIdentities) {
		err := tx.CreateTable(&Identity{}).Error
		if err != nil {
			return err
		}
	}

	// Insert version record
	kv := KeyValue{
		Key: keyVersion,
	}
	err := tx.Find(&kv).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint32(b, databaseVersion)
			kv.Value = b
			err = tx.Save(&kv).Error
		}
	}

	return err
}

func loadEncryptionKey(filepath string) (*[32]byte, error) {
	log.Tracef("loadEncryptionKey: %v", filepath)

	b, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("load encryption key %v: %v",
			filepath, err)
	}

	if hex.DecodedLen(len(b)) != 32 {
		return nil, fmt.Errorf("invalid key length %v",
			filepath)
	}

	k := make([]byte, 32)
	_, err = hex.Decode(k, b)
	if err != nil {
		return nil, fmt.Errorf("decode hex %v: %v",
			filepath, err)
	}

	var key [32]byte
	copy(key[:], k)
	util.Zero(k)

	return &key, nil
}

// New opens a connection to the CockroachDB user database and returns a new
// cockroachdb context. sslRootCert, sslCert, sslKey, and encryptionKey are
// file paths.
func New(host, network, sslRootCert, sslCert, sslKey, encryptionKey string) (*cockroachdb, error) {
	log.Tracef("New: %v %v %v %v %v %v", host, network, sslRootCert,
		sslCert, sslKey, encryptionKey)

	// Build url
	dbName := databaseID + "_" + network
	h := "postgresql://" + userPoliteiawww + "@" + host + "/" + dbName
	u, err := url.Parse(h)
	if err != nil {
		return nil, fmt.Errorf("parse url '%v': %v",
			h, err)
	}

	q := u.Query()
	q.Add("sslmode", "require")
	q.Add("sslrootcert", sslRootCert)
	q.Add("sslcert", sslCert)
	q.Add("sslkey", sslKey)
	u.RawQuery = q.Encode()

	// Connect to database
	db, err := gorm.Open("postgres", u.String())
	if err != nil {
		return nil, fmt.Errorf("connect to database '%v': %v",
			u.String(), err)
	}

	log.Infof("UserDB host: %v", h)

	// Load encryption key
	key, err := loadEncryptionKey(encryptionKey)
	if err != nil {
		return nil, err
	}

	// Create context
	c := &cockroachdb{
		userDB:        db,
		encryptionKey: key,
	}

	// Disable gorm logging. This prevents duplicate errors
	// from being printed since we handle errors manually.
	c.userDB.LogMode(false)

	// Disable automatic table name pluralization.
	// We set table names manually.
	c.userDB.SingularTable(true)

	// Setup database tables
	tx := c.userDB.Begin()
	err = c.createTables(tx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit().Error
	if err != nil {
		return nil, err
	}

	// Check version record
	kv := KeyValue{
		Key: keyVersion,
	}
	err = c.userDB.Find(&kv).Error
	if err != nil {
		return nil, fmt.Errorf("find version: %v", err)
	}

	// XXX A version mismatch will need to trigger a db
	// migration, but just return an error for now.
	version := binary.LittleEndian.Uint32(kv.Value)
	if version != databaseVersion {
		return nil, fmt.Errorf("version mismatch: got %v, want %v",
			version, databaseVersion)
	}

	return c, err
}
