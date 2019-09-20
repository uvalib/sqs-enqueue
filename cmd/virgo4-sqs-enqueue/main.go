package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handle from the queue name
	outQueueHandle, err := aws.QueueHandle( cfg.OutQueueName )
	if err != nil {
		log.Fatal( err )
	}

	count := uint( 0 )
    no_more_files := false

	// the block of messages to send
	block := make( []awssqs.Message, 0, awssqs.MAX_SQS_BLOCK_COUNT )

	for {

		// reset the block
		block = block[:0]

		start := time.Now()

		// attempt to load up to MAX_SQS_BLOCK_COUNT messages
		for {

			filename := fmt.Sprintf( "%s/message.%05d", cfg.InDir, count )
			contents, err := readMessage( filename )
			if err != nil {
				no_more_files = true
				break
			}

			// add a message to the block
			block = append( block, constructMessage( filename, contents ) )
			count++

			// have we done another complete block
			if count % awssqs.MAX_SQS_BLOCK_COUNT == 0 {
				break
			}
		}

		// do we have any files to process
		sz := uint( len( block ) )
		if sz != 0 {

			opStatus, err := aws.BatchMessagePut( outQueueHandle, block )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to send to outbound queue", ix )
				}
			}

			duration := time.Since(start)
			log.Printf("Processed %d messages (%0.2f tps)", sz, float64( sz ) / duration.Seconds() )
		}

		if no_more_files == true {
			log.Printf("No more files, terminating" )
			break
		}

		if cfg.MaxCount > 0 && count >= cfg.MaxCount  {
			log.Printf("Terminating after %d messages", count )
			break
		}
	}
}

func readMessage( filename string ) ( []byte, error ) {

	info, err := os.Stat( filename )
	if err != nil {
		return nil, err
	}

	sz := info.Size( )
	contents := make( []byte, sz )

	file, err := os.Open( filename )
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Read( contents )
	if err != nil {
		return nil, err
	}

	log.Printf("Read %s (%d bytes)", filename, sz )
	return contents, nil
}

func constructMessage( filename string, message []byte ) awssqs.Message {

	attributes := make( []awssqs.Attribute, 0, 1 )
	//attributes = append( attributes, awssqs.Attribute{ "op", "add" } )
	attributes = append( attributes, awssqs.Attribute{ "src", filename } )
	//attributes = append( attributes, awssqs.Attribute{ "type", "xml"} )
	return awssqs.Message{ Attribs: attributes, Payload: awssqs.Payload( message )}
}

//
// end of file
//