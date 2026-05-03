package main

import (
	"bytes"
	"cmp"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/simonvetter/modbus"
	"gopkg.in/yaml.v3"
)

type config struct {
	VMURL   string   `yaml:"VMURL"`
	Sources []source `yaml:"sources"`

	ModbusTimeout  time.Duration `yaml:"modbusTimeout"`
	PushTimeout    time.Duration `yaml:"pushTimeout"`
	UpdateDelay    time.Duration `yaml:"updateDelay"`
	CacheValidTime time.Duration `yaml:"cacheValidTime"`
	httpClient     *http.Client
	caches         []cache
	goodLength     map[*modbus.ModbusClient]map[uint16]uint16
	mutex          sync.Mutex
}

type cache struct {
	timestamp int64
	client    *modbus.ModbusClient
	start     uint16
	vals      []uint16
}

type source struct {
	Host         string        `yaml:"host"`
	Name         string        `yaml:"name"`
	Regs         []ri          `yaml:"registers"`
	Pause        time.Duration `yaml:"pause"`
	LowWordFirst bool          `yaml:"lowWordFirst"`
}

type ri struct {
	ID       uint16         `yaml:"ID`
	Name     string         `yaml:"name"`
	Desc     string         `yaml:"description"`
	OmType   string         `yaml:"openMetricType"`
	MbType   modbus.RegType `yaml:"modbusType"`
	IsSigned bool           `yaml:"isSigned"`
	Length   uint16         `yaml:"length"`
	Divisor  float64        `yaml:"divisor"`
	Unit     string         `yaml:"unit"`
}

var once sync.Once

func (c *config) pushToVM(lines []string) error {
	once.Do(func() {
		c.httpClient = &http.Client{}
	})

	buf := &bytes.Buffer{}
	for s := range lines {
		buf.Write([]byte(lines[s] + "\n"))
	}

	req, err := http.NewRequest("POST", c.VMURL, buf)
	if err != nil {
		fmt.Printf("error from NewRequest: %v", err)
		return err
	}
	_, err = c.httpClient.Do(req)

	if err != nil {
		fmt.Printf("error from Do: %v", err)
		return err
	}

	return err
}

func readConfig() *config {
	yamlData, err := os.ReadFile("config.yaml")
	if err != nil {
		panic("can't open/read config")
	}
	c := config{}

	err = yaml.Unmarshal(yamlData, &c)
	if err != nil {
		fmt.Printf("failed to read config: %v", err)
		panic("bye")
	}

	return &c
}

// getOpenClient makes sure to return a usable modbus client (or hang trying)
func getOpenClient(connectTo string, to time.Duration) *modbus.ModbusClient {
	var client *modbus.ModbusClient
	var err error

	for {
		client, err = modbus.NewClient(&modbus.ClientConfiguration{
			URL:     "tcp://" + connectTo,
			Timeout: to})

		if err == nil {
			break
		}
		fmt.Printf("Modbus connection to %s failed: %v\n",
			connectTo,
			err)

		time.Sleep(to)

	}

	for {
		err = client.Open()
		if err == nil {
			return client
		}

		fmt.Printf("Open for modbus client at %s failed: %v\n",
			connectTo,
			err)

		time.Sleep(to)
	}
}

// makeLine makes a line for the metric from the values
func makeLine(s source, r ri, vals []uint16) string {

	formatString := "%v {source=\"" + s.Name + "\"} %g %d"

	if r.Length == 1 {

		if r.IsSigned {
			return fmt.Sprintf(formatString, r.Name,
				float64(int16(vals[0]))/r.Divisor, time.Now().Unix()) //nolint:G115
		}
		return fmt.Sprintf(formatString, r.Name, float64(vals[0])/r.Divisor, time.Now().Unix())
	}

	if r.Length == 2 {
		v := uint32(vals[0])<<16 + uint32(vals[1])

		if s.LowWordFirst {
			v = uint32(vals[1])<<16 + uint32(vals[0])
		}
		if r.IsSigned {
			return fmt.Sprintf(formatString, r.Name,
				float64(int32(v))/r.Divisor, time.Now().Unix()) //nolint:G115
		}
		return fmt.Sprintf(formatString, r.Name, float64(v)/r.Divisor, time.Now().Unix())

	}
	return ""
}

func regCmp(a, b ri) int {
	return cmp.Compare(a.ID, b.ID)
}

// pollAndPush is an eternal loop polling data, pushing and sleeping
func pollAndPush(c *config, s source) {

	client := getOpenClient(s.Host, c.ModbusTimeout)

	slices.SortFunc(s.Regs, regCmp)

	for {
		time.Sleep(c.UpdateDelay)
		lines := make([]string, 0)

		for _, r := range s.Regs {

			vals, err := c.doReadRegisters(client, r.ID, r.Length, r.MbType)
			if err != nil {
				fmt.Printf("Error: poll failed  %v\n", err)
				continue
			}
			lines = append(lines, fmt.Sprintf("# TYPE %v %v", r.Name, r.OmType))
			lines = append(lines, fmt.Sprintf("# HELP %v %v", r.Name, r.Desc))
			lines = append(lines, fmt.Sprintf("# UNIT %v %v", r.Name, r.Unit))
			lines = append(lines, makeLine(s, r, vals))

			time.Sleep(s.Pause)
		}
		err := c.pushToVM(lines)
		if err != nil {
			fmt.Printf("pushing to metrics collection failed: %v", err)
		}
	}
}

// main is the starting function, but it just launches the loops
func main() {
	c := readConfig()

	for _, s := range c.Sources {
		go pollAndPush(c, s)
	}

	select {}
}

// cleanInvalidCaches removes expired cache entries, assumes we have lock
func (c *config) cleanInvalidCaches() {
	cvt := int64(c.CacheValidTime.Seconds())
	n := 0
	for n < len(c.caches) {
		if c.caches[n].timestamp < (time.Now().Unix() - cvt) {
			// Expired
			c.caches = append(c.caches[:n], c.caches[n+1:]...)
		} else {
			n++
		}
	}
}

// inCache checks if an appropriate value is in cache (invalid caches have
// been cleaned separately )
func (c *config) inCache(client *modbus.ModbusClient, base, count uint16) ([]uint16, error) {
	if c.caches == nil {
		c.caches = make([]cache, 0)
	}

	for n := range c.caches {
		if c.caches[n].client == client {
			l := uint16(len(c.caches[n].vals)) //nolint:G115
			if (c.caches[n].start <= base && c.caches[n].start+l >= base) &&
				(c.caches[n].start+l >= base+count) {
				// Use cached value
				index := base - c.caches[n].start
				return c.caches[n].vals[index : index+count], nil
			}
		}
	}
	return nil, fmt.Errorf("not found")
}

func (c *config) doReadRegisters(
	client *modbus.ModbusClient,
	base,
	count uint16,
	t modbus.RegType) ([]uint16, error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cleanInvalidCaches()

	vals, err := c.inCache(client, base, count)

	if err == nil {
		return vals, nil
	}

	if c.goodLength == nil {
		c.goodLength = make(map[*modbus.ModbusClient]map[uint16]uint16)
	}

	clientGoodLengths, ok := c.goodLength[client]
	if !ok {

		clientGoodLengths = make(map[uint16]uint16)
		c.goodLength[client] = clientGoodLengths
	}

	toRead, ok := clientGoodLengths[base]
	if !ok {
		toRead = uint16(64)
	}

	i := 0

	for i < 400 {
		var v []uint16
		v, err = client.ReadRegisters(base, toRead, t)
		if err == nil {

			// Note for future use
			clientGoodLengths[base] = toRead

			c.caches = append(c.caches, cache{
				client:    client,
				start:     base,
				vals:      v,
				timestamp: time.Now().Unix(),
			})

			return v[:count], err
		}

		fmt.Printf("Read at %v of %v entries failed with %v (count %v)\n", base, toRead, err, i)
		time.Sleep(c.ModbusTimeout)

		i++
		toRead /= 2 // try less next time
		if toRead <= count || toRead > 256 {
			// But we need to fulfill the request
			toRead = count
		}
	}

	return nil, err
}
