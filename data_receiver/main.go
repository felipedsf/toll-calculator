package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/felipedsf/toll-calculator/types"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
)

var kafkaTopic = "obudata"

func main() {
	recv, err := NewDataReceiver()
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/ws", recv.handlerWS)
	if err := http.ListenAndServe(":30000", nil); err != nil {
		log.Fatal(err)
	}
}

type DataReceiver struct {
	msgCh chan types.OBUData
	conn  *websocket.Conn
	prod  *kafka.Producer
}

func NewDataReceiver() (*DataReceiver, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
	})
	if err != nil {
		return nil, err
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return &DataReceiver{
		msgCh: make(chan types.OBUData, 128),
		conn:  nil,
		prod:  p,
	}, nil
}

func (dr DataReceiver) dataProducer(d types.OBUData) error {
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	err = dr.prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          b,
	}, nil)

	return err
}

func (dr *DataReceiver) handlerWS(w http.ResponseWriter, r *http.Request) {
	u := websocket.Upgrader{
		ReadBufferSize:  1028,
		WriteBufferSize: 1028,
	}
	conn, err := u.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}
	dr.conn = conn
	go dr.wsReceiveLoop()
}

func (dr *DataReceiver) wsReceiveLoop() {
	fmt.Println("New OBU connected client!")
	for {
		var data types.OBUData
		if err := dr.conn.ReadJSON(&data); err != nil {
			log.Println("Read error:", err)
			continue
		}
		fmt.Printf("received OBU date from [%d] lat: %.2f, long: %.2f\n", data.OBUID, data.Lat, data.Long)

		if err := dr.dataProducer(data); err != nil {
			log.Println("Error producing message:", err)
		}
	}

}
