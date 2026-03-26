package ssh

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/term"
)

// Options holds SSH connection parameters.
type Options struct {
	Host    string
	Port    int
	User    string
	KeyPath string
	Sudo    bool // prefix privileged commands with sudo (for non-root users)
}

// Client wraps an SSH connection and provides helpers for remote execution
// and file transfer.
type Client struct {
	opts   Options
	client *ssh.Client
}

// NewClient establishes an SSH connection using key-based auth, falling back
// to interactive password entry if no key is available.
func NewClient(opts Options) (*Client, error) {
	if opts.User == "" {
		opts.User = "root"
	}
	if opts.Port == 0 {
		opts.Port = 22
	}

	authMethods, err := buildAuthMethods(opts.KeyPath)
	if err != nil {
		return nil, err
	}

	cfg := &ssh.ClientConfig{
		User:            opts.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // operator tool; host key pinning is out of scope
		Timeout:         30 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	client, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", addr, err)
	}

	return &Client{opts: opts, client: client}, nil
}

// Close closes the underlying SSH connection.
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// sudoWrap prepends "sudo " to a command when the Sudo option is set.
func (c *Client) sudoWrap(cmd string) string {
	if c.opts.Sudo {
		return "sudo " + cmd
	}
	return cmd
}

// Run executes a command on the remote machine and returns its combined output.
func (c *Client) Run(cmd string) (string, error) {
	sess, err := c.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("creating SSH session: %w", err)
	}
	defer sess.Close()

	out, err := sess.CombinedOutput(c.sudoWrap(cmd))
	if err != nil {
		return string(out), fmt.Errorf("running %q: %w (output: %s)", cmd, err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// RunStream executes a command and streams stdout/stderr to the provided writers.
func (c *Client) RunStream(cmd string, stdout, stderr io.Writer) error {
	sess, err := c.client.NewSession()
	if err != nil {
		return fmt.Errorf("creating SSH session: %w", err)
	}
	defer sess.Close()

	sess.Stdout = stdout
	sess.Stderr = stderr

	return sess.Run(c.sudoWrap(cmd))
}

// RunIgnoreError executes a command and returns output, suppressing non-zero exit codes.
func (c *Client) RunIgnoreError(cmd string) string {
	out, _ := c.Run(cmd)
	return out
}

// FileExists returns true if the given path exists on the remote machine.
func (c *Client) FileExists(path string) bool {
	out, err := c.Run(fmt.Sprintf("test -e %q && echo yes || echo no", path))
	return err == nil && strings.TrimSpace(out) == "yes"
}

// Download copies a remote file to a local destination.
// When Sudo is enabled, the file is first staged to a world-readable temp
// path so that SFTP (which runs as the SSH user) can access it.
func (c *Client) Download(remotePath, localPath string) error {
	if !c.opts.Sudo {
		return c.sftpDownload(remotePath, localPath)
	}

	tmp := fmt.Sprintf("/tmp/.k3sto-%d", time.Now().UnixNano())
	if _, err := c.Run(fmt.Sprintf("cp %q %q", remotePath, tmp)); err != nil {
		return fmt.Errorf("staging %s for download: %w", remotePath, err)
	}
	if _, err := c.Run(fmt.Sprintf("chmod 644 %q", tmp)); err != nil {
		c.RunIgnoreError(fmt.Sprintf("rm -f %q", tmp))
		return fmt.Errorf("chmod staging file: %w", err)
	}
	defer c.RunIgnoreError(fmt.Sprintf("rm -f %q", tmp))
	return c.sftpDownload(tmp, localPath)
}

// Upload copies a local file to a remote path via SFTP.
// When Sudo is enabled, the file is first uploaded to /tmp then moved into place.
func (c *Client) Upload(localPath, remotePath string) error {
	if !c.opts.Sudo {
		return c.sftpUpload(localPath, remotePath)
	}

	tmp := fmt.Sprintf("/tmp/.k3sto-%d", time.Now().UnixNano())
	if err := c.sftpUpload(localPath, tmp); err != nil {
		return err
	}
	_, err := c.Run(fmt.Sprintf("mv %q %q", tmp, remotePath))
	return err
}

// UploadBytes writes the given content to a remote path via SFTP.
// When Sudo is enabled, content is first written to /tmp then moved into place.
func (c *Client) UploadBytes(content []byte, remotePath string) error {
	if !c.opts.Sudo {
		return c.sftpUploadBytes(content, remotePath)
	}

	tmp := fmt.Sprintf("/tmp/.k3sto-%d", time.Now().UnixNano())
	if err := c.sftpUploadBytes(content, tmp); err != nil {
		return err
	}
	_, err := c.Run(fmt.Sprintf("mv %q %q", tmp, remotePath))
	return err
}

// IsDisconnectError returns true if the error indicates an SSH connection drop
// (expected when the remote machine reboots).
func IsDisconnectError(err error) bool {
	if err == nil {
		return false
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	// ExitMissingError occurs when the remote process is killed (e.g. syscall.Reboot)
	// before it can send an SSH exit status.
	var exitMissing *ssh.ExitMissingError
	if errors.As(err, &exitMissing) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "use of closed network connection")
}

// sftpDownload downloads a remote file to a local path via SFTP.
func (c *Client) sftpDownload(remotePath, localPath string) error {
	sc, err := sftp.NewClient(c.client)
	if err != nil {
		return fmt.Errorf("opening SFTP session: %w", err)
	}
	defer sc.Close()

	src, err := sc.Open(remotePath)
	if err != nil {
		return fmt.Errorf("opening remote file %s: %w", remotePath, err)
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(localPath), 0750); err != nil {
		return fmt.Errorf("creating local directory: %w", err)
	}

	dst, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("creating local file %s: %w", localPath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("downloading %s: %w", remotePath, err)
	}
	return nil
}

// sftpUpload copies a local file to a remote path via SFTP.
func (c *Client) sftpUpload(localPath, remotePath string) error {
	sc, err := sftp.NewClient(c.client)
	if err != nil {
		return fmt.Errorf("opening SFTP session: %w", err)
	}
	defer sc.Close()

	src, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening local file %s: %w", localPath, err)
	}
	defer src.Close()

	dst, err := sc.OpenFile(remotePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return fmt.Errorf("creating remote file %s: %w", remotePath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("uploading to %s: %w", remotePath, err)
	}
	return nil
}

// sftpUploadBytes writes content to a remote path via SFTP.
func (c *Client) sftpUploadBytes(content []byte, remotePath string) error {
	sc, err := sftp.NewClient(c.client)
	if err != nil {
		return fmt.Errorf("opening SFTP session: %w", err)
	}
	defer sc.Close()

	dst, err := sc.OpenFile(remotePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return fmt.Errorf("creating remote file %s: %w", remotePath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("uploading to %s: %w", remotePath, err)
	}
	return nil
}

// buildAuthMethods constructs SSH auth methods: key-based first, password fallback.
func buildAuthMethods(keyPath string) ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod

	// 1. Try the provided key path, then common defaults.
	candidates := []string{}
	if keyPath != "" {
		candidates = append(candidates, keyPath)
	}
	home, _ := os.UserHomeDir()
	for _, name := range []string{"id_ed25519", "id_rsa", "id_ecdsa"} {
		candidates = append(candidates, filepath.Join(home, ".ssh", name))
	}

	for _, p := range candidates {
		if signer, err := loadPrivateKey(p); err == nil {
			methods = append(methods, ssh.PublicKeys(signer))
			break
		}
	}

	// 2. Password fallback.
	methods = append(methods, ssh.PasswordCallback(func() (string, error) {
		fmt.Printf("SSH password for %s: ", keyPath)
		pw, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		return string(pw), err
	}))

	return methods, nil
}

func loadPrivateKey(path string) (ssh.Signer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ssh.ParsePrivateKey(data)
}
