package config

import "testing"

func TestParseParameters(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		valid  bool
	}{
		{"defaults", nil, true},
		{"external", map[string]string{"externalEndpoints": `{"db-1":{"host":"db.example.test","port":5433,"serverName":"db-rw"}}`}, true},
		{"ipv6", map[string]string{"externalEndpoints": `{"db-1":{"host":"2001:db8::1","port":5432}}`}, true},
		{"internal TLS", map[string]string{"serverName": "postgres.example.test"}, true},
		{"unknown parameter", map[string]string{"typo": "ignored?"}, false},
		{"bad endpoint field", map[string]string{"externalEndpoints": `{"db-1":{"host":"host","port":5432,"ports":3}}`}, false},
		{"missing port", map[string]string{"externalEndpoints": `{"db-1":{"host":"host"}}`}, false},
		{"host includes port", map[string]string{"externalEndpoints": `{"db-1":{"host":"host:5432","port":5432}}`}, false},
		{"trailing object", map[string]string{"externalEndpoints": `{} {}`}, false},
		{"url", map[string]string{"externalEndpoints": `{"db-1":{"host":"https://host","port":5432}}`}, false},
		{"bad TLS name", map[string]string{"serverName": "host:5432"}, false},
		{"shared replica listener", map[string]string{"externalEndpoints": `{"db-1":{"host":"lb","port":5432},"db-2":{"host":"lb","port":5432}}`}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseParameters(tt.values)
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v err=%v", tt.valid, err)
			}
		})
	}
}
