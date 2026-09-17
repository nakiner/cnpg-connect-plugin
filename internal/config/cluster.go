package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

const PluginName = "connect.cnpg.io"

const (
	EnabledAnnotation    = PluginName + "/enabled"
	ParametersAnnotation = PluginName + "/parameters"
)

// Parameters are carried by Cluster.spec.plugins[].parameters. External
// endpoints must identify individual instances, not a shared replica listener.
type Parameters struct {
	ExternalEndpoints map[string]v1.Endpoint
	ServerName        string
}

func ParseParameters(values map[string]string) (Parameters, error) {
	p := Parameters{ExternalEndpoints: map[string]v1.Endpoint{}}
	for key, value := range values {
		switch key {
		case "externalEndpoints":
			dec := json.NewDecoder(strings.NewReader(value))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&p.ExternalEndpoints); err != nil {
				return p, fmt.Errorf("externalEndpoints: %w", err)
			}
			if err := dec.Decode(new(any)); err != io.EOF {
				return p, fmt.Errorf("externalEndpoints must contain one JSON object")
			}
		case "serverName":
			if value == "" || strings.ContainsAny(value, "/ \\?#@:\t\r\n") {
				return p, fmt.Errorf("serverName must be a DNS name")
			}
			p.ServerName = value
		default:
			return p, fmt.Errorf("unknown parameter %q", key)
		}
	}
	addresses := make(map[string]string)
	for name, endpoint := range p.ExternalEndpoints {
		if name == "" || endpoint.Host == "" || endpoint.Port == 0 || strings.ContainsAny(endpoint.Host, "/ \\?#@\t\r\n") {
			return p, fmt.Errorf("externalEndpoints[%q] requires an instance name, valid host and nonzero port", name)
		}
		if strings.Contains(endpoint.Host, ":") && net.ParseIP(endpoint.Host) == nil {
			return p, fmt.Errorf("externalEndpoints[%q]: use separate host and port fields", name)
		}
		if strings.ContainsAny(endpoint.ServerName, "/ \\?#@:\t\r\n") {
			return p, fmt.Errorf("externalEndpoints[%q]: serverName must be a DNS name", name)
		}
		address := net.JoinHostPort(strings.ToLower(endpoint.Host), strconv.Itoa(int(endpoint.Port)))
		if previous, exists := addresses[address]; exists {
			return p, fmt.Errorf("externalEndpoints[%q] and [%q] share an address; each endpoint must identify one instance", previous, name)
		}
		addresses[address] = name
	}
	return p, nil
}
