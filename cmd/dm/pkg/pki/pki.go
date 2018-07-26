// imported from kubernetes/kubeadm and modified by dotmesh-io

/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pki

import (
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"path"

	certutil "github.com/dotmesh-io/dotmesh/cmd/dm/pkg/cert"
)

type Configuration struct {
	AdvertiseAddresses []string
	ExternalDNSNames   []string
	ExtantCA           bool
}

func newCertificateAuthority() (*rsa.PrivateKey, *x509.Certificate, error) {
	key, err := certutil.NewPrivateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create private key [%v]", err)
	}

	config := certutil.Config{
		CommonName: "dotmesh",
	}

	cert, err := certutil.NewSelfSignedCACert(config, key)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create self-signed certificate [%v]", err)
	}

	return key, cert, nil
}

func newServerKeyAndCert(cfg *Configuration, caCert *x509.Certificate, caKey *rsa.PrivateKey, altNames certutil.AltNames) (*rsa.PrivateKey, *x509.Certificate, error) {
	key, err := certutil.NewPrivateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create private key [%v]", err)
	}

	internalAPIServerFQDN := []string{}

	internalAPIServerVirtualIP := net.ParseIP("127.0.0.1")

	altNames.IPs = append(altNames.IPs, internalAPIServerVirtualIP)
	altNames.DNSNames = append(altNames.DNSNames, internalAPIServerFQDN...)

	config := certutil.Config{
		CommonName: "dotmesh-etcd",
		AltNames:   altNames,
	}
	cert, err := certutil.NewSignedCert(config, key, caCert, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to sign certificate [%v]", err)
	}

	return key, cert, nil
}

func newClientKeyAndCert(caCert *x509.Certificate, caKey *rsa.PrivateKey) (*rsa.PrivateKey, *x509.Certificate, error) {
	key, err := certutil.NewPrivateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create private key [%v]", err)
	}

	config := certutil.Config{
		CommonName: "dotmesh-admin",
	}
	cert, err := certutil.NewSignedCert(config, key, caCert, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to sign certificate [%v]", err)
	}

	return key, cert, nil
}

func writeKeysAndCert(pkiPath string, name string, key *rsa.PrivateKey, cert *x509.Certificate) error {
	publicKeyPath, privateKeyPath, certificatePath := pathsKeysCerts(pkiPath, name)

	if key != nil {
		if err := certutil.WriteKey(privateKeyPath, certutil.EncodePrivateKeyPEM(key)); err != nil {
			return fmt.Errorf("unable to write private key file (%q) [%v]", privateKeyPath, err)
		}
		if pubKey, err := certutil.EncodePublicKeyPEM(&key.PublicKey); err == nil {
			if err := certutil.WriteKey(publicKeyPath, pubKey); err != nil {
				return fmt.Errorf("unable to write public key file (%q) [%v]", publicKeyPath, err)
			}
		} else {
			return fmt.Errorf("unable to encode public key to PEM [%v]", err)
		}
	}

	if cert != nil {
		if err := certutil.WriteCert(certificatePath, certutil.EncodeCertPEM(cert)); err != nil {
			return fmt.Errorf("unable to write certificate file (%q) [%v]", certificatePath, err)
		}
	}

	return nil
}

func pathsKeysCerts(pkiPath, name string) (string, string, string) {
	return path.Join(pkiPath, fmt.Sprintf("%s-pub.pem", name)),
		path.Join(pkiPath, fmt.Sprintf("%s-key.pem", name)),
		path.Join(pkiPath, fmt.Sprintf("%s.pem", name))
}

// CreatePKIAssets will create and write to disk all PKI assets necessary to establish the control plane.
// It first generates a self-signed CA certificate, a server certificate (signed by the CA) and a key for
// signing service account tokens. It returns CA key and certificate, which is convenient for use with
// client config funcs.
func CreatePKIAssets(pkiPath string, cfg *Configuration) (*rsa.PrivateKey, *x509.Certificate, error) {
	var (
		err      error
		altNames certutil.AltNames
		caKey    *rsa.PrivateKey
		caCert   *x509.Certificate
	)

	for _, a := range cfg.AdvertiseAddresses {
		if ip := net.ParseIP(a); ip != nil {
			altNames.IPs = append(altNames.IPs, ip)
		} else {
			return nil, nil, fmt.Errorf("could not parse ip %q", a)
		}
	}
	altNames.DNSNames = append(altNames.DNSNames, cfg.ExternalDNSNames...)

	_, prv, cert := pathsKeysCerts(pkiPath, "ca")

	if !cfg.ExtantCA {
		// initialize a new CA
		caKey, caCert, err = newCertificateAuthority()
		if err != nil {
			return nil, nil, fmt.Errorf("failure while creating CA keys and certificate - %v", err)
		}
		if err := writeKeysAndCert(pkiPath, "ca", caKey, caCert); err != nil {
			return nil, nil, fmt.Errorf("failure while saving CA keys and certificate - %v", err)
		}
	} else {
		// try to load existing CA, and use that to sign new "api" server key
		caCerts, err := certutil.CertsFromFile(cert)
		if err != nil {
			return nil, nil, err
		}
		// assume we're dealing with the ca cert that we created, which always
		// has one item. XXX this will be problematic if we want to support
		// user-provided cert chains.
		caCert = caCerts[0]
		caKeyBytes, err := ioutil.ReadFile(prv)
		if err != nil {
			return nil, nil, err
		}
		caKeyInterface, err := certutil.ParsePrivateKeyPEM(caKeyBytes)
		if err != nil {
			return nil, nil, err
		}
		caKeyCasted, ok := caKeyInterface.(*rsa.PrivateKey)
		if !ok {
			return nil, nil, fmt.Errorf("Unable to cast %v (from %v) to *rsa.PrivateKey", caKey, prv)
		}
		caKey = caKeyCasted
	}

	apiKey, apiCert, err := newServerKeyAndCert(cfg, caCert, caKey, altNames)
	if err != nil {
		return nil, nil, fmt.Errorf("failure while creating API server keys and certificate - %v", err)
	}
	if err := writeKeysAndCert(pkiPath, "apiserver", apiKey, apiCert); err != nil {
		return nil, nil, fmt.Errorf("failure while saving API server keys and certificate - %v", err)
	}
	_, prv, cert = pathsKeysCerts(pkiPath, "apiserver")

	return caKey, caCert, nil
}
