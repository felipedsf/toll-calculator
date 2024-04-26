package main

import (
	"github.com/felipedsf/toll-calculator/types"
	"github.com/gorilla/websocket"
	"log"
	"math"
	"math/rand"
	"time"
)

const (
	wsEndpoint   = "ws://127.0.0.1:30000/ws"
	sendInterval = time.Second
)

func genLatLong() (float64, float64) {
	return genCoord(), genCoord()
}

func genCoord() float64 {
	n := float64(rand.Intn(100) + 1)
	f := rand.Float64()
	return n + f
}

func main() {
	obuIDS := generateOBUIDS(20)
	conn, _, err := websocket.DefaultDialer.Dial(wsEndpoint, nil)
	if err != nil {
		log.Fatal(err)
	}

	for {
		for _, obuID := range obuIDS {
			lat, long := genLatLong()
			data := types.OBUData{
				OBUID: obuID,
				Lat:   lat,
				Long:  long,
			}
			if err := conn.WriteJSON(data); err != nil {
				log.Fatal(err)
			}
		}
		time.Sleep(sendInterval)
	}
}

func generateOBUIDS(n int) []int {
	ids := make([]int, n)
	for i := 0; i < n; i++ {
		ids[i] = rand.Intn(math.MaxInt)
	}
	return ids
}

func init() {

}
