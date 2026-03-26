package nextboot

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"

	_ "embed"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/rothgar/k3s-to-talos/internal/ssh"
)

//go:embed assets/nextboot-talos.py.tmpl
var scriptTemplate string

// Options holds parameters for the nextboot-talos installer.
type Options struct {
	TalosVersion   string
	ControlPlaneIP string
	ConfigFile     string // path to local controlplane.yaml
}

// Installer uploads and runs nextboot-talos on the remote machine.
type Installer struct {
	ssh       *ssh.Client
	backupDir string
}

// NewInstaller creates a new Installer.
func NewInstaller(sshClient *ssh.Client, backupDir string) *Installer {
	return &Installer{ssh: sshClient, backupDir: backupDir}
}

// Run generates, uploads, and executes the nextboot-talos script.
func (i *Installer) Run(opts Options) error {
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)

	// 1. Resolve the Talos image URL and hash
	s.Suffix = " Resolving Talos image URL..."
	s.Start()

	imageURL, imageHash, err := resolveImageInfo(opts.TalosVersion)
	if err != nil {
		s.Stop()
		color.Yellow("  Warning: could not fetch image hash (%v); proceeding without hash verification.\n", err)
		imageURL = fmt.Sprintf(
			"https://github.com/siderolabs/talos/releases/download/%s/metal-amd64.raw.xz",
			opts.TalosVersion,
		)
		imageHash = ""
	}
	s.Stop()
	fmt.Printf("  Image URL:  %s\n", imageURL)
	if imageHash != "" {
		fmt.Printf("  SHA256:     %s\n", imageHash)
	}

	// 2. Read the machine config
	s.Suffix = " Reading machine config..."
	s.Start()

	configContent := ""
	if opts.ConfigFile != "" {
		data, err := os.ReadFile(opts.ConfigFile)
		if err != nil {
			s.Stop()
			color.Yellow("  Warning: could not read config file %s: %v\n", opts.ConfigFile, err)
		} else {
			configContent = string(data)
		}
	}
	s.Stop()

	// 3. Render the script template
	script, err := renderScript(scriptParams{
		Version:       opts.TalosVersion,
		ImageURL:      imageURL,
		HashValue:     imageHash,
		ConfigContent: escapeForPython(configContent),
		Reboot:        "True",
	})
	if err != nil {
		return fmt.Errorf("rendering nextboot-talos script: %w", err)
	}

	// 4. Upload script to remote
	s.Suffix = " Uploading nextboot-talos script..."
	s.Start()

	remotePath := "/tmp/nextboot-talos.py"
	if err := i.ssh.UploadBytes([]byte(script), remotePath); err != nil {
		s.Stop()
		return fmt.Errorf("uploading nextboot-talos: %w", err)
	}
	if _, err := i.ssh.Run(fmt.Sprintf("chmod +x %s", remotePath)); err != nil {
		s.Stop()
		return fmt.Errorf("chmod nextboot-talos: %w", err)
	}
	s.Stop()
	fmt.Printf("  ✓ Script uploaded to %s\n", remotePath)

	// 5. Verify python3 is available on remote
	if _, err := i.ssh.Run("which python3"); err != nil {
		return fmt.Errorf("python3 not found on remote machine (required by nextboot-talos)\n" +
			"Install it with: apt-get install -y python3")
	}

	// 6. Execute the script (streaming output)
	color.Red("\n  !! POINT OF NO RETURN — executing nextboot-talos !!\n")
	color.Red("  The remote machine will now be erased and rebooted into Talos.\n\n")

	err = i.ssh.RunStream(
		fmt.Sprintf("python3 %s 2>&1", remotePath),
		newPrefixWriter("  remote> "),
		newPrefixWriter("  remote> "),
	)

	// The SSH connection dropping is expected (machine reboots)
	if err != nil && !ssh.IsDisconnectError(err) {
		return fmt.Errorf("nextboot-talos script failed: %w", err)
	}

	return nil
}

// scriptParams holds values to substitute into the Python template.
type scriptParams struct {
	Version       string
	ImageURL      string
	HashValue     string
	ConfigContent string
	Reboot        string
}

func renderScript(p scriptParams) (string, error) {
	tmpl, err := template.New("nextboot").Parse(scriptTemplate)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, p); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// resolveImageInfo fetches the Talos release page to get the image URL and SHA256 hash.
func resolveImageInfo(version string) (imageURL, hash string, err error) {
	// Use the metal-amd64 raw image (x86_64 bare metal)
	imageURL = fmt.Sprintf(
		"https://github.com/siderolabs/talos/releases/download/%s/metal-amd64.raw.xz",
		version,
	)

	// Fetch the SHA256 checksums file
	checksumURL := fmt.Sprintf(
		"https://github.com/siderolabs/talos/releases/download/%s/sha256sum.txt",
		version,
	)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(checksumURL)
	if err != nil {
		return imageURL, "", fmt.Errorf("fetching checksums: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return imageURL, "", fmt.Errorf("fetching checksums: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return imageURL, "", fmt.Errorf("reading checksums: %w", err)
	}

	// Parse sha256sum.txt — format: "<hash>  <filename>"
	for _, line := range strings.Split(string(body), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && strings.Contains(parts[1], "metal-amd64.raw.xz") {
			return imageURL, parts[0], nil
		}
	}

	return imageURL, "", fmt.Errorf("metal-amd64.raw.xz not found in checksums")
}

// hashFile computes the SHA256 of a local file.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// escapeForPython escapes a string for embedding in a Python triple-quoted string.
func escapeForPython(s string) string {
	// Escape backslashes and triple-quotes
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, `"""`, `\"\"\"`)
	return s
}

// prefixWriter is an io.Writer that prepends a string to each line.
type prefixWriter struct {
	prefix  string
	buf     []byte
}

func newPrefixWriter(prefix string) *prefixWriter {
	return &prefixWriter{prefix: prefix}
}

func (pw *prefixWriter) Write(p []byte) (n int, err error) {
	pw.buf = append(pw.buf, p...)
	for {
		idx := bytes.IndexByte(pw.buf, '\n')
		if idx < 0 {
			break
		}
		line := pw.buf[:idx+1]
		fmt.Printf("%s%s", pw.prefix, line)
		pw.buf = pw.buf[idx+1:]
	}
	return len(p), nil
}

// Ensure hashFile is referenced (it may be used in future verification).
var _ = hashFile
