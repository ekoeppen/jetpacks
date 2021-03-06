package swap

import (
	"encoding/binary"
	"math"
	"errors"
	"fmt"
)

type SwapFunction byte

const (
	STATUS  SwapFunction = 0
	QUERY                = 1
	COMMAND              = 2
)

type SwapValueType string

const (
	INT8    SwapValueType = "int8"
	UINT8                 = "uint8"
	INT16                 = "int16"
	UINT16                = "uint16"
	INT32                 = "int32"
	UINT32                = "uint32"
	FLOAT                 = "float"
	CSTRING               = "cstring"
	PSTRING               = "pstring"
)

type SwapPacket struct {
	RSSI		byte
	LQI		byte
	Source          byte
	Destination     byte
	Hops            byte
	Security        byte
	Nonce           byte
	Function        SwapFunction
	RegisterAddress byte
	RegisterID      byte
	Payload         []byte
}

type SwapRegister struct {
	Address byte
	RawData []byte
}

type SwapValue struct {
	Name     string
	Register byte
	Position byte
	Type     SwapValueType
	Unit     string
	Offset   int
	Scale    int
	RawData  []byte
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

func hexBytesToSwapFunction(data []byte) SwapFunction {
	if data[0] == 48 {
		if data[1] == 48 {
			return STATUS
		} else if data[1] == 49 {
			return QUERY
		} else if data[1] == 50 {
			return COMMAND
		}
	}
	return STATUS
}

// Decode a SWAP packed from raw packet data
func (p *SwapPacket) Decode(data []byte) error {
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
		return nil
	}
	return errors.New("Invalid SWAP data")
}

// Copy the value to a byte array representation
func (value *SwapValue) SetRawData(data []byte) {
	var l byte
	switch value.Type {
	case INT8:
		l = 1
	case UINT8:
		l = 1
	case INT16:
		l = 2
	case UINT16:
		l = 2
	case INT32:
		l = 4
	case UINT32:
		l = 4
	}
	value.RawData = data[value.Position : value.Position+l]
}

// Return the value as an integer, return an error if the value is not of integer type
func (value *SwapValue) AsInt() (n int64, err error) {
	switch value.Type {
	case INT8:
		n = int64(value.RawData[0])
	case UINT8:
		n = int64(value.RawData[0])
	case INT16:
		n = int64(binary.BigEndian.Uint16(value.RawData[0:2]))
	case UINT16:
		n = int64(binary.BigEndian.Uint16(value.RawData[0:2]))
	case INT32:
		n = int64(binary.BigEndian.Uint32(value.RawData[0:4]))
	case UINT32:
		n = int64(binary.BigEndian.Uint32(value.RawData[0:4]))
	default:
		err = errors.New("Value not an integer")
	}
	return
}

func (value *SwapValue) String() string {
	n, err := value.AsInt()
	if err == nil {
		if value.Scale == 1 {
			return fmt.Sprintf("%d", n - int64(value.Offset))
		} else {
			w := int(math.Log10(float64(value.Scale)))
			return fmt.Sprintf("%.*f", w, float64(n) / float64(value.Scale) - float64(value.Offset))
		}
	}
	return ""
}

type SwapMote struct {
	Address   byte
	Location  string
	Registers []SwapRegister
	Values    []SwapValue
}

// Update SWAP values of a mote from a SWAP packet and return the values as a mapping of value names to value as strings
func (mote *SwapMote) UpdateValues(p *SwapPacket) map[string]string {
	values := make(map[string]string)
	for _, value := range mote.Values {
		if value.Register == p.RegisterID {
			value.SetRawData(p.Payload)
			values[mote.Location + "/" + value.Name] = value.String();
		}
	}
	return values
}
