package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	OutQueueName      string
	MessageBucketName string
	InDir             string
	MaxCount          uint
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig
	flag.StringVar(&cfg.OutQueueName, "outqueue", "", "Output queue name")
	flag.StringVar(&cfg.MessageBucketName, "bucket", "", "Oversize message bucket name")
	flag.StringVar(&cfg.InDir, "indir", "", "Input directory name")
	flag.UintVar(&cfg.MaxCount, "max", 0, "Maximum number of records to enqueue (0 is all of them)")

	flag.Parse()

	if len(cfg.OutQueueName) == 0 {
		log.Fatalf("OutQueueName cannot be blank")
	}
	if len(cfg.MessageBucketName) == 0 {
		log.Fatalf("MessageBucketName cannot be blank")
	}
	if len(cfg.InDir) == 0 {
		log.Fatalf("InDir cannot be blank")
	}

	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName)
	log.Printf("[CONFIG] MessageBucketName    = [%s]", cfg.MessageBucketName)
	log.Printf("[CONFIG] InDir                = [%s]", cfg.InDir)
	log.Printf("[CONFIG] MaxCount             = [%d]", cfg.MaxCount)

	return &cfg
}

//
// end of file
//
