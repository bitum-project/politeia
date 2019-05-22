// Copyright (c) 2017-2019 The Bitum developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package testcache

import (
	bitum "github.com/bitum-project/politeia/bitumplugin"
	"github.com/bitum-project/politeia/politeiad/cache"
)

func (c *testcache) getComments(payload string) (string, error) {
	gc, err := bitum.DecodeGetComments([]byte(payload))
	if err != nil {
		return "", err
	}

	c.RLock()
	defer c.RUnlock()

	gcrb, err := bitum.EncodeGetCommentsReply(
		bitum.GetCommentsReply{
			Comments: c.comments[gc.Token],
		})
	if err != nil {
		return "", err
	}

	return string(gcrb), nil
}

func (c *testcache) authorizeVote(cmdPayload, replyPayload string) (string, error) {
	av, err := bitum.DecodeAuthorizeVote([]byte(cmdPayload))
	if err != nil {
		return "", err
	}

	avr, err := bitum.DecodeAuthorizeVoteReply([]byte(replyPayload))
	if err != nil {
		return "", err
	}

	av.Receipt = avr.Receipt
	av.Timestamp = avr.Timestamp

	c.Lock()
	defer c.Unlock()

	_, ok := c.authorizeVotes[av.Token]
	if !ok {
		c.authorizeVotes[av.Token] = make(map[string]bitum.AuthorizeVote)
	}

	c.authorizeVotes[av.Token][avr.RecordVersion] = *av

	return replyPayload, nil
}

func (c *testcache) startVote(cmdPayload, replyPayload string) (string, error) {
	sv, err := bitum.DecodeStartVote([]byte(cmdPayload))
	if err != nil {
		return "", err
	}

	svr, err := bitum.DecodeStartVoteReply([]byte(replyPayload))
	if err != nil {
		return "", err
	}

	c.Lock()
	defer c.Unlock()

	// Store start vote data
	c.startVotes[sv.Vote.Token] = *sv
	c.startVoteReplies[sv.Vote.Token] = *svr

	return replyPayload, nil
}

func (c *testcache) voteDetails(payload string) (string, error) {
	vd, err := bitum.DecodeVoteDetails([]byte(payload))
	if err != nil {
		return "", err
	}

	c.Lock()
	defer c.Unlock()

	// Lookup the latest record version
	r, err := c.record(vd.Token)
	if err != nil {
		return "", err
	}

	// Prepare reply
	_, ok := c.authorizeVotes[vd.Token]
	if !ok {
		c.authorizeVotes[vd.Token] = make(map[string]bitum.AuthorizeVote)
	}

	vdb, err := bitum.EncodeVoteDetailsReply(
		bitum.VoteDetailsReply{
			AuthorizeVote:  c.authorizeVotes[vd.Token][r.Version],
			StartVote:      c.startVotes[vd.Token],
			StartVoteReply: c.startVoteReplies[vd.Token],
		})
	if err != nil {
		return "", err
	}

	return string(vdb), nil
}

func (c *testcache) bitumExec(cmd, cmdPayload, replyPayload string) (string, error) {
	switch cmd {
	case bitum.CmdGetComments:
		return c.getComments(cmdPayload)
	case bitum.CmdAuthorizeVote:
		return c.authorizeVote(cmdPayload, replyPayload)
	case bitum.CmdStartVote:
		return c.startVote(cmdPayload, replyPayload)
	case bitum.CmdVoteDetails:
		return c.voteDetails(cmdPayload)
	}

	return "", cache.ErrInvalidPluginCmd
}
