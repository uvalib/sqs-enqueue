package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[0])

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{})
	if err != nil {
		log.Fatal(err)
	}

	// get the queue handle from the queue name
	outQueueHandle, err := aws.QueueHandle(cfg.OutQueueName)
	if err != nil {
		log.Fatal(err)
	}

	count := uint(0)
	no_more_files := false

	// the block of messages to send
	block := make([]awssqs.Message, 0, awssqs.MAX_SQS_BLOCK_COUNT)

	for {

		// reset the block
		block = block[:0]

		start := time.Now()

		// attempt to load up to MAX_SQS_BLOCK_COUNT messages
		for {

			message, err := loadMessage(cfg, count)
			if err != nil {
				no_more_files = true
				break
			}

			// add a message to the block
			block = append(block, *message)
			count++

			// have we done another complete block
			if count%awssqs.MAX_SQS_BLOCK_COUNT == 0 {
				break
			}
		}

		// do we have any files to process
		sz := uint(len(block))
		if sz != 0 {

			opStatus, err := aws.BatchMessagePut(outQueueHandle, block)
			if err != nil {
				log.Fatal(err)
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf("WARNING: message %d failed to send to outbound queue", ix)
				}
			}

			duration := time.Since(start)
			log.Printf("Processed %d messages (%0.2f tps)", sz, float64(sz)/duration.Seconds())
		}

		if no_more_files == true {
			log.Printf("No more files (%d processed), terminating", count)
			break
		}

		if cfg.MaxCount > 0 && count >= cfg.MaxCount {
			log.Printf("Terminating after %d messages", count)
			break
		}
	}
}

func loadMessage(config *ServiceConfig, index uint) (*awssqs.Message, error) {

	payloadName := fmt.Sprintf("%s/payload.%05d", config.InDir, index)
	attribsName := fmt.Sprintf("%s/attribs.%05d", config.InDir, index)

	info, err := os.Stat(attribsName)
	if err != nil {
		return nil, err
	}

	info, err = os.Stat(payloadName)
	if err != nil {
		return nil, err
	}

	sz := info.Size()
	contents := make([]byte, sz)

	payloadFile, err := os.Open(payloadName)
	if err != nil {
		return nil, err
	}
	defer payloadFile.Close()

	_, err = payloadFile.Read(contents)
	if err != nil {
		return nil, err
	}

	message := &awssqs.Message{Payload: contents}

	attribsFile, err := os.Open(attribsName)
	if err != nil {
		return nil, err
	}
	defer attribsFile.Close()
	reader := bufio.NewReader(attribsFile)
	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		// split at the first = character and assign to the attributes
		tokens := strings.SplitN(line, "=", 2)
		if tokens != nil {
			message.Attribs = append(message.Attribs, awssqs.Attribute{Name: tokens[0], Value: tokens[1]})
		}
	}

	return message, nil
}

//
// end of file
//
