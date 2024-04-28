package main

import (
	"fmt"
	"github.com/felipedsf/toll-calculator/data_receiver/producer"
	"github.com/felipedsf/toll-calculator/types"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
)

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
	prod  producer.DataProducer
}

func NewDataReceiver() (*DataReceiver, error) {
	var (
		p   producer.DataProducer
		err error
	)

	p, err = producer.NewKafkaProducer()
	if err != nil {
		return nil, err
	}

	p = newLogMiddleware(p)
	return &DataReceiver{
		msgCh: make(chan types.OBUData, 128),
		conn:  nil,
		prod:  p,
	}, nil
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

func (dr *DataReceiver) ProduceData(data types.OBUData) error {
	return dr.prod.ProduceData(data)
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

		if err := dr.ProduceData(data); err != nil {
			log.Println("Error producing message:", err)
		}
	}

}
