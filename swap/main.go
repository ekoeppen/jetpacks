package main

import (
	"flag"
	client "jetpacks/lib/client"
	swap "jetpacks/lib/swap"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

var motes []swap.SwapMote
var subHub *client.Client
var pubHub *client.Client

func main() {
	var publishTo, subscribeFrom, topic, cert, key, ca, moteDir string

	// omit timestamps from the Log and send output to stdout
	log.SetFlags(log.Flags() & ^log.Ldate & ^log.Ltime)
	log.SetOutput(os.Stdout)

	// get some info from the environment, set when hub starts this pack
	packName := os.Getenv("HUB_PACK")
	if packName == "" {
		packName = "swap"
	}
	subscribeFrom = os.Getenv("HUB_MQTT")
	if subscribeFrom == "" {
		subscribeFrom = "tcp://localhost:1883"
	}

	// command line parameters
	flag.StringVar(&subscribeFrom, "sub", subscribeFrom, "Broker for subscription")
	flag.StringVar(&topic, "topic", "logger", "Topic for subscription")
	flag.StringVar(&publishTo, "pub", "", "Broker for publishing")
	flag.StringVar(&moteDir, "motes", "swapmotes", "Folder with mote definitions")
	flag.StringVar(&cert, "cert", "client.crt", "Client certificate file for TLS connections")
	flag.StringVar(&key, "key", "client.key", "Client key file for TLS connections")
	flag.StringVar(&ca, "ca", "ca.crt", "Certificate Authority file for broker certification")
	flag.Parse()

	if publishTo == "" {
		publishTo = subscribeFrom
	}
	// normal pack startup begins here
	log.Printf("SWAP pack starting...\n    Listening for topic %s, mote directory at %s\n", topic, moteDir)
	log.Printf("    Subscribing from %s, Publishing to %s\n", subscribeFrom, publishTo)
	if strings.HasPrefix(subscribeFrom, "tcps") || strings.HasPrefix(publishTo, "tcps") {
		log.Printf("    Using TLS with certificate %s, key %s and CA %s\n", cert, key, ca)
	}

	readMotes(moteDir)

	// connect to MQTT and wait for it before doing anything else
	subHub = &client.Client{}
	subHub.ConnectToHub(packName, subscribeFrom, true, cert, key, ca)
	if subscribeFrom == publishTo {
		pubHub = subHub
	} else {
		pubHub = &client.Client{}
		pubHub.ConnectToHub(packName, publishTo, true, cert, key, ca)
	}

	go swapListener(topic)

	done := make(chan struct{})
	<-done // hang around forever
}

func readMotes(moteDir string) {
	files, _ := filepath.Glob(moteDir + "/*")
	motes = make([]swap.SwapMote, len(files))
	for i, file := range files {
		log.Printf("Reading from %s\n", file)
		viper.SetConfigFile(file)
		err := viper.ReadInConfig()
		if err != nil {
			log.Fatalf("Failed to parse mote config: %v\n", err)
		}
		viper.UnmarshalKey("general", &motes[i])
		viper.UnmarshalKey("values", &motes[i].Values)
		log.Printf("Mote %d: Location %s\n", motes[i].Address, motes[i].Location)
		for _, value := range motes[i].Values {
			log.Printf("    Value: %s, type: %v\n", value.Name, value.Type)
		}
	}
}

func sendToHub(topic, value string) {
	pubHub.SendToHub(topic, value, true)
	log.Printf("%s: %s\n", topic, value)
}

func handlePacket(p *swap.SwapPacket) {
	for _, mote := range motes {
		if mote.Address == p.RegisterAddress {
			sendToHub("SWAP/"+mote.Location+"/Timestamp", strconv.FormatInt(time.Now().UTC().Unix(), 10))
			sendToHub("SWAP/"+mote.Location+"/CC_RSSI", strconv.FormatInt(int64(p.RSSI), 10))
			sendToHub("SWAP/"+mote.Location+"/LQI", strconv.FormatInt(int64(p.LQI), 10))
			for i, v := range mote.UpdateValues(p) {
				sendToHub("SWAP/"+i, v)
			}
		}
	}
}

func swapListener(feed string) {
	for evt := range subHub.TopicWatcher(feed) {
		var p swap.SwapPacket
		if err := p.Decode(evt.Payload); err == nil {
			handlePacket(&p)
		}
	}
}
