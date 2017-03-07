package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mitchellh/mapstructure"
)

type Client struct {
	hub      mqtt.Client
	clientID string
}

// connectToHub sets up an MQTT client and registers as a "jet/..." client.
// Uses last-will to automatically unregister on disconnect. This returns a
// "topic notifier" channel to allow updating the registered status value.
func (c *Client) ConnectToHub(clientName, port string, retain bool, cert, key, ca string) chan<- interface{} {
	// add a "fairly random" 6-digit suffix to make the client name unique
	nanos := time.Now().UnixNano()
	c.clientID = fmt.Sprintf("%s/%06d", clientName, nanos%1e6)
	options := mqtt.NewClientOptions()
	options.AddBroker(port)
	options.SetClientID(c.clientID)
	options.SetKeepAlive(10)
	options.SetBinaryWill("jet/"+c.clientID, nil, 1, retain)

	url, err := url.Parse(port)
	if err != nil {
		log.Fatalf("Malformed URL: %v\n", err)
	}
	if url.Scheme == "tcps" {
		cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			log.Fatalf("Failed to load client cert: %v\n", err)
		}
		caCert, err := ioutil.ReadFile(ca)
		if err != nil {
			log.Fatalf("Failed to read CA cert: %v\n", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig := &tls.Config{
			ServerName:   url.Hostname(),
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()
		options.SetTLSConfig(tlsConfig)
	}

	c.hub = mqtt.NewClient(options)

	if t := c.hub.Connect(); t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}

	if retain {
		log.Println("connected as", c.clientID, "to", port)
	}

	// register as jet client, cleared on disconnect by the will
	feed := c.TopicNotifier("jet/"+c.clientID, retain)
	feed <- 0 // start off with state "0" to indicate connection

	// return a topic feed to allow publishing hub status changes
	return feed
}

// sendToHub publishes a message, and waits for it to complete successfully.
// Note: does no JSON conversion if the payload is already a []byte.
func (c *Client) SendToHub(topic string, payload interface{}, retain bool) {
	data, ok := payload.([]byte)
	if !ok {
		var e error
		data, e = json.Marshal(payload)
		if e != nil {
			log.Println("json conversion failed:", e, payload)
			return
		}
	}
	t := c.hub.Publish(topic, 1, retain, data)
	if t.Wait() && t.Error() != nil {
		log.Print(t.Error())
	}
}

type Event struct {
	Topic    string
	Payload  []byte
	Retained bool
}

func (e *Event) Decode(result interface{}) bool {
	var payload interface{}
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		log.Println("json decode error:", err, e.Payload)
		return false
	}
	if err := mapstructure.WeakDecode(payload, result); err != nil {
		log.Println("decode error:", err, e)
		return false
	}
	return true
}

// topicWatcher turns an MQTT subscription into a channel feed of Events.
func (c *Client) TopicWatcher(pattern string) <-chan Event {
	feed := make(chan Event)

	t := c.hub.Subscribe(pattern, 0, func(hub mqtt.Client, msg mqtt.Message) {
		feed <- Event{
			Topic:    msg.Topic(),
			Payload:  msg.Payload(),
			Retained: msg.Retained(),
		}
	})
	if t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}

	return feed
}

// topicNotifier returns a channel which publishes all its messages to MQTT.
func (c *Client) TopicNotifier(topic string, retain bool) chan<- interface{} {
	feed := make(chan interface{})

	go func() {
		for msg := range feed {
			c.SendToHub(topic, msg, retain)
		}
	}()

	return feed
}
