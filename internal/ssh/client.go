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
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/term"
)

// Options holds SSH connection parameters.
type Options struct {
	Host    string
	Port    int
	User    string
	KeyPath string
	Sudo    bool // prefix privileged commands with sudo; auto-set when User != "root"
	Verbose bool // print each command and its output to stderr
}

// Client wraps an SSH connection and provides helpers for remote execution
// and file transfer.
type Client struct {
	opts         Options
	client       *ssh.Client
	sudoPassword string // cached password for sudo -S; empty when NOPASSWD is configured
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
	// Automatically use sudo for non-root users.
	if opts.User != "root" {
		opts.Sudo = true
	}

	authMethods, err := buildAuthMethods(opts.KeyPath)
	if err != nil {
		return nil, err
	}

	cfg := &ssh.ClientConfig{
		User:            opts.User,
		Auth:            authMethods,
		HostKeyCallback: buildHostKeyCallback(),
		Timeout:         30 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	sshClient, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, formatDialError(opts.Host, addr, err)
	}

	c := &Client{opts: opts, client: sshClient}

	// For non-root users, check whether sudo needs a password and prompt once.
	if opts.Sudo {
		if needsPw, err := c.sudoNeedsPassword(); err == nil && needsPw {
			pw, err := promptSudoPassword(opts.User, opts.Host)
			if err != nil {
				sshClient.Close()
				return nil, err
			}
			c.sudoPassword = pw
		}
	}

	return c, nil
}

// Close closes the underlying SSH connection.
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// logCmd prints the command being executed when verbose mode is enabled.
// The prompt character mirrors shell convention: '#' for root, '$' otherwise.
func (c *Client) logCmd(raw, wrapped string) {
	if !c.opts.Verbose {
		return
	}
	prompt := "$"
	if c.opts.Sudo || c.opts.User == "root" {
		prompt = "#"
	}
	fmt.Fprintf(os.Stderr, "\n[ssh %s@%s] %s %s\n", c.opts.User, c.opts.Host, prompt, raw)
}

// logOutput prints command output when verbose mode is enabled.
func (c *Client) logOutput(out string) {
	if !c.opts.Verbose || strings.TrimSpace(out) == "" {
		return
	}
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		fmt.Fprintf(os.Stderr, "    %s\n", line)
	}
}

// sudoWrap prepends sudo to a command when the Sudo option is set.
// Uses "sudo -S -p ''" when a cached password is available so the password
// can be supplied via stdin without any prompt text contaminating the output.
func (c *Client) sudoWrap(cmd string) string {
	if !c.opts.Sudo {
		return cmd
	}
	if c.sudoPassword != "" {
		return "sudo -S -p '' " + cmd
	}
	return "sudo " + cmd
}

// Run executes a command on the remote machine and returns its combined output.
func (c *Client) Run(cmd string) (string, error) {
	sess, err := c.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("creating SSH session: %w", err)
	}
	defer sess.Close()

	wrapped := c.sudoWrap(cmd)
	c.logCmd(cmd, wrapped)
	if c.sudoPassword != "" {
		sess.Stdin = strings.NewReader(c.sudoPassword + "\n")
	}
	out, err := sess.CombinedOutput(wrapped)
	c.logOutput(string(out))
	if err != nil {
		return string(out), fmt.Errorf("running %q: %w (output: %s)", cmd, err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// RunNoSudo executes a command without the sudo prefix, regardless of the
// Sudo option.  Use for read-only detection checks that work without root.
func (c *Client) RunNoSudo(cmd string) (string, error) {
	sess, err := c.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("creating SSH session: %w", err)
	}
	defer sess.Close()

	c.logCmd(cmd, cmd)
	out, err := sess.CombinedOutput(cmd)
	c.logOutput(string(out))
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

	wrapped := c.sudoWrap(cmd)
	c.logCmd(cmd, wrapped)
	sess.Stdout = stdout
	sess.Stderr = stderr
	if c.sudoPassword != "" {
		sess.Stdin = strings.NewReader(c.sudoPassword + "\n")
	}

	return sess.Run(wrapped)
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

	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		os.Remove(localPath) // remove partial file so callers cannot mistake it for a complete download
		return fmt.Errorf("downloading %s: %w", remotePath, err)
	}
	return dst.Close()
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

// sudoNeedsPassword runs "sudo -n true" to check whether passwordless sudo is
// configured.  Returns (true, nil) when a password is required.
func (c *Client) sudoNeedsPassword() (bool, error) {
	sess, err := c.client.NewSession()
	if err != nil {
		return false, err
	}
	defer sess.Close()
	out, err := sess.CombinedOutput("sudo -n true 2>&1")
	if err == nil {
		return false, nil // NOPASSWD — no password needed
	}
	msg := strings.ToLower(string(out))
	if strings.Contains(msg, "password") || strings.Contains(msg, "askpass") {
		return true, nil
	}
	return false, fmt.Errorf("sudo check failed: %s", strings.TrimSpace(string(out)))
}

// promptSudoPassword asks the user to enter their sudo password interactively.
func promptSudoPassword(user, host string) (string, error) {
	fmt.Printf("[sudo] password for %s@%s: ", user, host)
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return "", fmt.Errorf("reading sudo password: %w", err)
	}
	return string(pw), nil
}

// buildHostKeyCallback returns a HostKeyCallback that verifies against
// ~/.ssh/known_hosts.  Unknown hosts (not yet in the file) are accepted
// silently so first-time connections work without manual intervention.
// If the host IS in known_hosts but the key no longer matches, the callback
// returns an error so the caller can show a clear remediation message.
// Falls back to InsecureIgnoreHostKey when known_hosts cannot be loaded.
func buildHostKeyCallback() ssh.HostKeyCallback {
	home, err := os.UserHomeDir()
	if err != nil {
		return ssh.InsecureIgnoreHostKey() //nolint:gosec
	}
	knownHostsPath := filepath.Join(home, ".ssh", "known_hosts")
	if _, err := os.Stat(knownHostsPath); err != nil {
		return ssh.InsecureIgnoreHostKey() //nolint:gosec
	}
	cb, err := knownhosts.New(knownHostsPath)
	if err != nil {
		return ssh.InsecureIgnoreHostKey() //nolint:gosec
	}
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		err := cb(hostname, remote, key)
		if err == nil {
			return nil
		}
		var keyErr *knownhosts.KeyError
		if errors.As(err, &keyErr) && len(keyErr.Want) == 0 {
			// Host not in known_hosts yet — allow the connection.
			return nil
		}
		// Key mismatch for a known host — surface the real error.
		return err
	}
}

// formatDialError wraps ssh.Dial errors with actionable remediation hints.
func formatDialError(host, addr string, err error) error {
	var keyErr *knownhosts.KeyError
	if errors.As(err, &keyErr) && len(keyErr.Want) > 0 {
		return fmt.Errorf(
			"connecting to %s: host key has changed (known_hosts mismatch).\n"+
				"If the server was rebuilt, remove the old entry and retry:\n"+
				"  ssh-keygen -R %s\n"+
				"Original error: %w",
			addr, host, err,
		)
	}
	return fmt.Errorf("connecting to %s: %w", addr, err)
}

// buildAuthMethods constructs SSH auth methods in priority order:
//  1. SSH agent (SSH_AUTH_SOCK) — handles passphrase-protected keys transparently
//  2. Key file (explicit path or common defaults) — for keys not in the agent
//  3. Password prompt — last resort fallback
func buildAuthMethods(keyPath string) ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod

	// When an explicit key is provided via --ssh-key, try it FIRST — before
	// the SSH agent.  The agent may hold many keys and the server's
	// MaxAuthTries (default 6, sometimes 3) can be exhausted by agent keys
	// before the explicit key gets a chance.
	if keyPath != "" {
		if signer, err := loadPrivateKey(keyPath); err == nil {
			methods = append(methods, ssh.PublicKeys(signer))
		}
	}

	// SSH agent — handles passphrase-protected keys transparently.
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		if conn, err := net.Dial("unix", sock); err == nil {
			methods = append(methods, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
		}
	}

	// Default key files (only when no explicit key was provided or it failed
	// to load — avoids double-trying the same key).
	if keyPath == "" {
		home, _ := os.UserHomeDir()
		for _, name := range []string{"id_ed25519", "id_rsa", "id_ecdsa"} {
			if signer, err := loadPrivateKey(filepath.Join(home, ".ssh", name)); err == nil {
				methods = append(methods, ssh.PublicKeys(signer))
				break
			}
		}
	}

	// Password fallback.
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
