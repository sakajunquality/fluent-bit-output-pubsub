package main

import (	
	"C"
	"context"
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"

	"cloud.google.com/go/pubsub"
)

var topic *pubsub.Topic

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "fluent-bit-output-pubsub", "FluentBit Output Plugin Cloud PubSub")
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	projectID := output.FLBPluginConfigKey(ctx, "project_id")
	topicName := output.FLBPluginConfigKey(ctx, "topic")

	ctxBg := context.Background()
	client, err := pubsub.NewClient(ctxBg, projectID)
	if err != nil {
		return output.FLB_ERROR
	}

	topic = client.Topic(topicName)
	ok, err := topic.Exists(ctxBg)
	if err != nil || !ok {
		fmt.Printf("topic %s was not found in project %s. err: %s", topicName, projectID, err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// Iterate Records
	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		timestamp := ts.(output.FLBTime)
		fmt.Printf("Publishing Record %s: %v", timestamp.String(), record)

		dataBuf, err := json.Marshal(record)
		if err != nil {
			return output.FLB_ERROR
		}

		ctxBg := context.Background()
		result := topic.Publish(ctxBg, &pubsub.Message{Data: dataBuf})
		_, err = result.Get(ctxBg)
		if err != nil {
			return output.FLB_RETRY
		}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	topic.Stop()
	return output.FLB_OK
}

func main() {
}
