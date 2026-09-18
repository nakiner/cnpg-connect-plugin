package observer

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// applicationDatabase follows CNPG's GetApplicationDatabaseName. The API
// object already contains CNPG bootstrap defaults; a restored cluster without
// application metadata has no inferred default database.
func applicationDatabase(cluster *unstructured.Unstructured) string {
	for _, method := range []string{"recovery", "pg_basebackup", "initdb"} {
		name, _, _ := unstructured.NestedString(cluster.Object, "spec", "bootstrap", method, "database")
		if name != "" {
			return name
		}
	}
	return ""
}

func serverCASecret(cluster *unstructured.Unstructured) string {
	name, _, _ := unstructured.NestedString(cluster.Object, "status", "certificates", "serverCASecret")
	return name
}

func (o *Observer) connectionParameters(ctx context.Context, cluster *unstructured.Unstructured) (v1.ConnectionParameters, error) {
	result := v1.ConnectionParameters{Database: applicationDatabase(cluster)}
	name := serverCASecret(cluster)
	if name == "" {
		return result, fmt.Errorf("PostgreSQL server CA not yet reported")
	}
	requestCtx, cancel := context.WithTimeout(ctx, o.opts.ProbeTimeout)
	defer cancel()
	secret, err := o.kube.CoreV1().Secrets(cluster.GetNamespace()).Get(requestCtx, name, metav1.GetOptions{})
	if err != nil {
		return result, fmt.Errorf("read PostgreSQL server CA: %w", err)
	}
	// Only serialize public certificates, even if a malformed ca.crt contains
	// unrelated PEM blocks. Never publish other Secret keys or private material.
	remaining := secret.Data["ca.crt"]
	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil {
			break
		}
		remaining = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return result, fmt.Errorf("invalid PostgreSQL server CA certificate")
		}
		result.ServerCAPEM = append(result.ServerCAPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes})...)
	}
	if len(result.ServerCAPEM) == 0 {
		return result, fmt.Errorf("PostgreSQL server CA has no public certificates")
	}
	return result, nil
}
