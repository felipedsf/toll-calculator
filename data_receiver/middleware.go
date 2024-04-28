package main

import (
	"github.com/felipedsf/toll-calculator/data_receiver/producer"
	"github.com/felipedsf/toll-calculator/types"
	"github.com/sirupsen/logrus"
	"time"
)

type LogMiddleware struct {
	next producer.DataProducer
}

func newLogMiddleware(next producer.DataProducer) *LogMiddleware {
	return &LogMiddleware{
		next: next,
	}
}

func (l *LogMiddleware) ProduceData(data types.OBUData) error {
	start := time.Now()
	defer func() {
		logrus.WithFields(logrus.Fields{
			"obuID": data.OBUID,
			"lat":   data.Lat,
			"long":  data.Long,
			"took":  time.Since(start),
		}).Info("Producing to kafka")
	}()

	return l.next.ProduceData(data)
}
