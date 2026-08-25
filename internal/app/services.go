package app

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Sygmei/LightningBNB/internal/mux"
)

// ClientService maps one local TCP port to a server service selector.
type ClientService struct {
	LocalPort int
	Remote    string
}

func ParseServerServices(values []string) ([]mux.Service, error) {
	services := make([]mux.Service, 0, len(values))
	seen := make(map[string]bool)
	for _, value := range values {
		name, portText, ok := strings.Cut(value, ":")
		name = strings.TrimSpace(name)
		portText = strings.TrimSpace(portText)
		if !ok || name == "" || portText == "" || strings.Contains(name, ":") {
			return nil, fmt.Errorf("invalid --service %q; expected NAME:PORT", value)
		}
		if len(name) > 64 {
			return nil, fmt.Errorf("invalid --service %q; service name is too long", value)
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("invalid --service %q; port must be between 1 and 65535", value)
		}
		if _, err := strconv.Atoi(name); err == nil {
			return nil, fmt.Errorf("invalid --service %q; service names must not be numeric", value)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate server service %q", name)
		}
		seen[name] = true
		services = append(services, mux.Service{Name: name, Port: port})
	}
	return services, nil
}

func ValidateClientServices(values []ClientService) error {
	seenPorts := make(map[int]bool)
	for _, service := range values {
		if service.LocalPort < 1 || service.LocalPort > 65535 {
			return fmt.Errorf("client service local port must be between 1 and 65535: %d", service.LocalPort)
		}
		if strings.TrimSpace(service.Remote) == "" || strings.Contains(service.Remote, ":") {
			return fmt.Errorf("client service target %q must be an alias or server port", service.Remote)
		}
		if len(service.Remote) > 64 {
			return fmt.Errorf("client service target %q is too long", service.Remote)
		}
		if port, err := strconv.Atoi(service.Remote); err == nil && (port < 1 || port > 65535) {
			return fmt.Errorf("client service target port must be between 1 and 65535: %d", port)
		}
		if seenPorts[service.LocalPort] {
			return fmt.Errorf("duplicate client service local port %d", service.LocalPort)
		}
		seenPorts[service.LocalPort] = true
	}
	return nil
}

// ClientServicesForAdvertised returns a local mapping for every service the
// server advertised. The local port deliberately matches the server port so
// the all-services mode is predictable without requiring another mapping.
func ClientServicesForAdvertised(values []mux.Service) ([]ClientService, error) {
	services := make([]ClientService, 0, len(values))
	seenPorts := make(map[int]bool)
	for _, service := range values {
		if service.Port < 1 || service.Port > 65535 {
			return nil, fmt.Errorf("advertised service %q has invalid port %d", service.Name, service.Port)
		}
		if seenPorts[service.Port] {
			return nil, fmt.Errorf("advertised services contain duplicate port %d; cannot expose both on the same local port", service.Port)
		}
		seenPorts[service.Port] = true

		selector := service.Name
		if selector == "" {
			selector = strconv.Itoa(service.Port)
		}
		services = append(services, ClientService{LocalPort: service.Port, Remote: selector})
	}
	return services, nil
}
