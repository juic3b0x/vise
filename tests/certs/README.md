# Public TLS test fixtures

`proxy-cert.der` and `proxy-key.der` are a self-signed localhost certificate and its test-only PKCS#8 private key. They are used only by the proxy integration fixtures. The private key is intentionally public and checked in and must never be used for a deployed service.

The certificate includes `localhost` and `127.0.0.1` subject alternative names. Tests explicitly add it to a local client trust store when checking successful TLS proxy routing; they also check that default clients reject it. Production builds do not embed or trust these files.

To regenerate both files with OpenSSL:

```sh
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /tmp/vise-proxy-test-key.pem -out /tmp/vise-proxy-test-cert.pem \
  -days 3650 -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -addext 'basicConstraints=critical,CA:FALSE' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
  -addext 'extendedKeyUsage=serverAuth'
openssl x509 -in /tmp/vise-proxy-test-cert.pem -outform DER -out tests/certs/proxy-cert.der
openssl pkcs8 -topk8 -nocrypt -in /tmp/vise-proxy-test-key.pem -outform DER -out tests/certs/proxy-key.der
```
