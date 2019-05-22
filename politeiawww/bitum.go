// Copyright (c) 2017-2019 The Bitum developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"github.com/bitum-project/politeia/bitumplugin"
	pd "github.com/bitum-project/politeia/politeiad/api/v1"
	"github.com/bitum-project/politeia/politeiad/cache"
	"github.com/bitum-project/politeia/util"
)

// bitumGetComment sends the bitum plugin getcomment command to the cache and
// returns the specified comment.
func (p *politeiawww) bitumGetComment(token, commentID string) (*bitumplugin.Comment, error) {
	// Setup plugin command
	gc := bitumplugin.GetComment{
		Token:     token,
		CommentID: commentID,
	}

	payload, err := bitumplugin.EncodeGetComment(gc)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdGetComment,
		CommandPayload: string(payload),
	}

	// Get comment from the cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	gcr, err := bitumplugin.DecodeGetCommentReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return &gcr.Comment, nil
}

// bitumGetComments sends the bitum plugin getcomments command to the cache
// and returns all of the comments for the passed in proposal token.
func (p *politeiawww) bitumGetComments(token string) ([]bitumplugin.Comment, error) {
	// Setup plugin command
	gc := bitumplugin.GetComments{
		Token: token,
	}

	payload, err := bitumplugin.EncodeGetComments(gc)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdGetComments,
		CommandPayload: string(payload),
	}

	// Get comments from the cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, fmt.Errorf("PluginExec: %v", err)
	}

	gcr, err := bitumplugin.DecodeGetCommentsReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return gcr.Comments, nil
}

// bitumCommentLikes sends the bitum plugin commentlikes command to the cache
// and returns all of the comment likes for the passed in comment.
func (p *politeiawww) bitumCommentLikes(token, commentID string) ([]bitumplugin.LikeComment, error) {
	// Setup plugin command
	cl := bitumplugin.CommentLikes{
		Token:     token,
		CommentID: commentID,
	}

	payload, err := bitumplugin.EncodeCommentLikes(cl)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdCommentLikes,
		CommandPayload: string(payload),
	}

	// Get comment likes from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	clr, err := bitumplugin.DecodeCommentLikesReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return clr.CommentLikes, nil
}

// bitumPropCommentLikes sends the bitum plugin proposalcommentslikes command
// to the cache and returns all of the comment likes for the passed in proposal
// token.
func (p *politeiawww) bitumPropCommentLikes(token string) ([]bitumplugin.LikeComment, error) {
	// Setup plugin command
	pcl := bitumplugin.GetProposalCommentsLikes{
		Token: token,
	}

	payload, err := bitumplugin.EncodeGetProposalCommentsLikes(pcl)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdProposalCommentsLikes,
		CommandPayload: string(payload),
	}

	// Get proposal comment likes from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	rp := []byte(reply.Payload)
	pclr, err := bitumplugin.DecodeGetProposalCommentsLikesReply(rp)
	if err != nil {
		return nil, err
	}

	return pclr.CommentsLikes, nil
}

// bitumVoteDetails sends the bitum plugin votedetails command to the cache
// and returns the vote details for the passed in proposal.
func (p *politeiawww) bitumVoteDetails(token string) (*bitumplugin.VoteDetailsReply, error) {
	// Setup plugin command
	vd := bitumplugin.VoteDetails{
		Token: token,
	}

	payload, err := bitumplugin.EncodeVoteDetails(vd)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdVoteDetails,
		CommandPayload: string(payload),
	}

	// Get vote details from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	vdr, err := bitumplugin.DecodeVoteDetailsReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return vdr, nil
}

// bitumProposalVotes sends the bitum plugin proposalvotes command to the
// cache and returns the vote results for the passed in proposal.
func (p *politeiawww) bitumProposalVotes(token string) (*bitumplugin.VoteResultsReply, error) {
	// Setup plugin command
	vr := bitumplugin.VoteResults{
		Token: token,
	}

	payload, err := bitumplugin.EncodeVoteResults(vr)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdProposalVotes,
		CommandPayload: string(payload),
	}

	// Get proposal votes from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	vrr, err := bitumplugin.DecodeVoteResultsReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return vrr, nil
}

// bitumInventory sends the bitum plugin inventory command to the cache and
// returns the bitum plugin inventory.
func (p *politeiawww) bitumInventory() (*bitumplugin.InventoryReply, error) {
	// Setup plugin command
	i := bitumplugin.Inventory{}
	payload, err := bitumplugin.EncodeInventory(i)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdInventory,
		CommandPayload: string(payload),
	}

	// Get cache inventory
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	ir, err := bitumplugin.DecodeInventoryReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return ir, nil
}

// bitumTokenInventory sends the bitum plugin tokeninventory command to the
// cache.
func (p *politeiawww) bitumTokenInventory(bestBlock uint64) (*bitumplugin.TokenInventoryReply, error) {
	payload, err := bitumplugin.EncodeTokenInventory(
		bitumplugin.TokenInventory{
			BestBlock: bestBlock,
		})
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdTokenInventory,
		CommandPayload: string(payload),
	}

	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	tir, err := bitumplugin.DecodeTokenInventoryReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return tir, nil
}

// bitumLoadVoteResults sends the loadvotesummaries command to politeiad.
func (p *politeiawww) bitumLoadVoteResults(bestBlock uint64) (*bitumplugin.LoadVoteResultsReply, error) {
	// Setup plugin command
	challenge, err := util.Random(pd.ChallengeSize)
	if err != nil {
		return nil, err
	}

	lvr := bitumplugin.LoadVoteResults{
		BestBlock: bestBlock,
	}
	payload, err := bitumplugin.EncodeLoadVoteResults(lvr)
	if err != nil {
		return nil, err
	}

	pc := pd.PluginCommand{
		Challenge: hex.EncodeToString(challenge),
		ID:        bitumplugin.ID,
		Command:   bitumplugin.CmdLoadVoteResults,
		CommandID: bitumplugin.CmdLoadVoteResults,
		Payload:   string(payload),
	}

	// Send plugin command to politeiad
	respBody, err := p.makeRequest(http.MethodPost,
		pd.PluginCommandRoute, pc)
	if err != nil {
		return nil, err
	}

	// Handle response
	var pcr pd.PluginCommandReply
	err = json.Unmarshal(respBody, &pcr)
	if err != nil {
		return nil, err
	}

	err = util.VerifyChallenge(p.cfg.Identity, challenge, pcr.Response)
	if err != nil {
		return nil, err
	}

	b := []byte(pcr.Payload)
	reply, err := bitumplugin.DecodeLoadVoteResultsReply(b)
	if err != nil {
		spew.Dump("here")
		return nil, err
	}

	return reply, nil
}

// bitumVoteSummary uses the bitum plugin vote summary command to request a
// vote summary for a specific proposal from the cache.
func (p *politeiawww) bitumVoteSummary(token string) (*bitumplugin.VoteSummaryReply, error) {
	v := bitumplugin.VoteSummary{
		Token: token,
	}
	payload, err := bitumplugin.EncodeVoteSummary(v)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             bitumplugin.ID,
		Command:        bitumplugin.CmdVoteSummary,
		CommandPayload: string(payload),
	}

	resp, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	reply, err := bitumplugin.DecodeVoteSummaryReply([]byte(resp.Payload))
	if err != nil {
		return nil, err
	}

	return reply, nil
}
