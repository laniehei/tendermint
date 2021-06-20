package stream

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/pubsub/query"
	"github.com/tendermint/tendermint/state/indexer"
	"github.com/tendermint/tendermint/types"
)

var _ indexer.EventSink = (*EventSink)(nil)

const (
	TableEventBlock = "block_events"
	TableEventTx    = "tx_events"
	TableResultTx   = "tx_results"
	DriverName      = "consumer"
)

// EventSink is an indexer
// backend providing the
// tx/block index services.
type EventSink struct {
	store   sarama.Consumer
	chainID string
}

func NewEventSink(
	connStr string,
	chainID string,
) (indexer.EventSink, sarama.Consumer, error) {

	// TODO: add config.
	config := sarama.Config{}

	c, err := sarama.NewConsumer(
		[]string{connStr},
		&config,
	)
	if err != nil {
		return &EventSink{}, c, err
	}

	return &EventSink{
		chainID: chainID,
	}, c, nil
}

func (es *EventSink) Type() indexer.EventSinkType {
	return indexer.CONSUMER
}

// To be implemented ...
func (es *EventSink) IndexBlockEvents(h types.EventDataNewBlockHeader) error {
	return nil

}

// To be implemented ...
func (es *EventSink) IndexTxEvents(txr []*abci.TxResult) error {
	return nil
}

func (es *EventSink) SearchBlockEvents(ctx context.Context, q *query.Query) ([]int64, error) {
	return nil, errors.New("block search is not supported via the stream event sink")
}

func (es *EventSink) SearchTxEvents(ctx context.Context, q *query.Query) ([]*abci.TxResult, error) {
	return nil, errors.New("tx search is not supported via the stream event sink")
}

func (es *EventSink) GetTxByHash(hash []byte) (*abci.TxResult, error) {
	return nil, errors.New("getTxByHash is not supported via the stream event sink")
}

func (es *EventSink) HasBlock(h int64) (bool, error) {
	return false, errors.New("hasBlock is not supported via the stream event sink")
}

func (es *EventSink) Stop() error {
	return es.store.Close()
}
