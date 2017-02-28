package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"
	"strconv"
	"errors"
	"strings"
	"flag"
	swap "jetpacks/lib/swap"
	client "jetpacks/lib/client"

	"github.com/spf13/viper"
)

var motes []swap.SwapMote

func main() {
	var publishTo, subscribeFrom, topic, cert, key, moteDir string

	// omit timestamps from the Log and send output to stdout
	log.SetFlags(log.Flags() & ^log.Ldate & ^log.Ltime)
	log.SetOutput(os.Stdout)

	// get some info from the environment, set when hub starts this pack
	packName := os.Getenv("HUB_PACK")
	if packName == "" {
		packName = "swap"
	}
	mqttPort := os.Getenv("HUB_MQTT")
	if mqttPort == "" {
		mqttPort = "tcp://localhost:1883"
	}

	// command line parameters
	flag.StringVar(&subscribeFrom, "sub", mqttPort, "Broker for subscription")
	flag.StringVar(&topic, "topic", "logger", "Topic for subscription")
	flag.StringVar(&publishTo, "pub", mqttPort, "Broker for publishing")
	flag.StringVar(&moteDir, "motes", "swap_motes", "Folder with mote definitions")
	flag.StringVar(&cert, "cert", "client.crt", "Client certificate file for TLS connections")
	flag.StringVar(&key, "key", "client.key", "Client key file for TLS connections")
	flag.Parse()

	// normal pack startup begins here
	log.Printf("SWAP pack starting...\n    Listening for topic %s, mote directory at %s\n", topic, moteDir)
	log.Printf("    Subscribing from %s, Publishing to %s\n", subscribeFrom, publishTo)
	if strings.HasPrefix(subscribeFrom, "tcps") || strings.HasPrefix(publishTo, "tcps") {
		log.Printf("    Using TSL with certificate %s and key %s\n", cert, key)
	}

	readMotes(moteDir)

	// connect to MQTT and wait for it before doing anything else
	client.ConnectToHub(packName, mqttPort, true)

	go swapListener(os.Args[1])

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

func hexByteToByte(data byte) byte {
	if data >= 97 {
		return data - 97 + 10
	} else if data >= 65 {
		return data - 65 + 10
	} else {
		return data - 48
	}
}

func hexBytesToByte(data []byte) byte {
	return hexByteToByte(data[0])*16 + hexByteToByte(data[1])
}

func hexBytesToSwapFunction(data []byte) swap.SwapFunction {
	if data[0] == 48 {
		if data[1] == 48 {
			return swap.STATUS
		} else if data[1] == 49 {
			return swap.QUERY
		} else if data[1] == 50 {
			return swap.COMMAND
		}
	}
	return swap.STATUS
}

func handlePacket(p *swap.SwapPacket) {
	for _, mote := range motes {
		if mote.Address == p.RegisterAddress {
			for i, v := range(mote.UpdateValues(p)) {
				log.Printf("%s: %s\n", "SWAP/" + i, v)
			}
			log.Printf("%s %s", "SWAP/" + mote.Location + "/Timestamp", strconv.FormatInt(time.Now().UTC().Unix(), 10))
			log.Printf("%s %s", "SWAP/" + mote.Location + "/CC_RSSI", strconv.FormatInt(int64(p.RSSI), 10))
			log.Printf("%s %s", "SWAP/" + mote.Location + "/LQI", strconv.FormatInt(int64(p.LQI), 10))
		}
	}
}

func decodeSwapData(data []byte, p *swap.SwapPacket) error {
	minLength := 1+2*2+1+7*2
	l := len(data)
	if l >= minLength {
		p.RSSI = hexBytesToByte(data[1:3])
		p.LQI = hexBytesToByte(data[3:5])
		p.Source = hexBytesToByte(data[6:8])
		p.Destination = hexBytesToByte(data[8:10])
		p.Hops = hexByteToByte(data[10])
		p.Security = hexByteToByte(data[11])
		p.Nonce = hexBytesToByte(data[12:14])
		p.Function = hexBytesToSwapFunction(data[14:16])
		p.RegisterAddress = hexBytesToByte(data[16:18])
		p.RegisterID = hexBytesToByte(data[18:20])
		if l > minLength {
			p.Payload = make([]byte, (l-minLength)/2, (l-minLength)/2)
			for i, _ := range p.Payload {
				p.Payload[i] = hexBytesToByte(data[minLength+i*2 : minLength+(i+1)*2])
			}
		}
		s, _ := json.Marshal(p)
		log.Printf("%s\n", s)
		return nil
	}
	return errors.New("Invalid SWAP data")
}

func swapListener(feed string) {
	for evt := range client.TopicWatcher(feed) {
		var p swap.SwapPacket
		if err := decodeSwapData(evt.Payload, &p); err == nil {
			handlePacket(&p)
		}
	}
}

