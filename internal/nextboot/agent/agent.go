// Package agent implements the Talos disk-imaging logic that runs on the
// target (remote) machine.  It is invoked via the hidden "nextboot"
// subcommand after the k3s-to-talos binary is uploaded over SSH.
package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const ioBlockSize = 4 * 1024 * 1024 // 4 MiB

// Options controls what the nextboot agent does.
type Options struct {
	ImageURL  string // required: URL of the Talos raw disk image (.raw.zst / .raw.xz / .raw)
	ImageHash string // optional: expected lowercase hex SHA-256 of the downloaded (compressed) file
	Config    string // optional: local path to the Talos machine config (controlplane.yaml)
	Disk      string // optional: target block device; auto-detected when empty
	Reboot    bool   // reboot immediately after writing
}

func log(format string, args ...any) {
	fmt.Printf("[nextboot] "+format+"\n", args...)
}

// Run executes the full Talos installation on the local machine.
// Must be called as root.
func Run(opts Options) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("nextboot must run as root (uid=%d)", os.Geteuid())
	}
	if opts.ImageURL == "" {
		return fmt.Errorf("--image-url is required")
	}

	// ── 1. Detect boot disk ──────────────────────────────────────────────────
	disk := opts.Disk
	if disk == "" {
		var err error
		disk, err = detectBootDisk()
		if err != nil {
			return fmt.Errorf("detecting boot disk: %w", err)
		}
	}
	log("Boot disk  : %s", disk)
	log("Image URL  : %s", opts.ImageURL)
	log("═══════════════════════════════════════════════════════════")

	// ── 2. Ensure required decompressor is present ───────────────────────────
	if strings.HasSuffix(opts.ImageURL, ".zst") {
		if err := ensureTool("zstd"); err != nil {
			return err
		}
	} else if strings.HasSuffix(opts.ImageURL, ".xz") {
		if err := ensureTool("xz"); err != nil {
			return err
		}
	}

	// ── 3. Download, decompress, write to disk in one streaming pipeline ─────
	log("Starting download → decompress → disk pipeline...")
	log("  !! This will ERASE all data on %s. Starting in 5 seconds !!", disk)
	for i := 5; i > 0; i-- {
		fmt.Printf("  %d...\n", i)
		time.Sleep(time.Second)
	}

	if err := streamImageToDisk(opts.ImageURL, opts.ImageHash, disk); err != nil {
		return fmt.Errorf("imaging disk: %w", err)
	}
	log("Disk write complete.")

	// ── 4. Write machine config to STATE partition ───────────────────────────
	if opts.Config != "" {
		configData, err := os.ReadFile(opts.Config)
		if err != nil {
			log("Warning: could not read config %s: %v", opts.Config, err)
			log("Talos will boot in maintenance mode.")
		} else if err := writeConfig(disk, configData); err != nil {
			log("Warning: %v", err)
			log("Talos will boot in maintenance mode.")
			log("  talosctl apply-config --insecure --nodes <ip> --file controlplane.yaml")
		}
	}

	log("═══════════════════════════════════════════════════════════")
	log("Talos installation complete.")

	// ── 5. Reboot ────────────────────────────────────────────────────────────
	if opts.Reboot {
		log("Rebooting into Talos Linux...")
		time.Sleep(2 * time.Second)
		return reboot()
	}
	log("AUTO_REBOOT disabled — run 'reboot' manually to boot into Talos.")
	return nil
}

// ── Disk detection ───────────────────────────────────────────────────────────

func detectBootDisk() (string, error) {
	if out, err := exec.Command("findmnt", "-n", "-o", "SOURCE", "/").Output(); err == nil {
		src := strings.TrimSpace(string(out))
		disk := regexp.MustCompile(`(p\d+|\d+)$`).ReplaceAllString(src, "")
		if disk != "" {
			if _, err := os.Stat(disk); err == nil {
				return disk, nil
			}
		}
	}
	if out, err := exec.Command("lsblk", "-dno", "NAME,TYPE").Output(); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if fields := strings.Fields(line); len(fields) == 2 && fields[1] == "disk" {
				return "/dev/" + fields[0], nil
			}
		}
	}
	return "", fmt.Errorf("could not auto-detect boot disk; specify --disk explicitly")
}

// ── Tool installation ────────────────────────────────────────────────────────

func ensureTool(name string) error {
	if _, err := exec.LookPath(name); err == nil {
		return nil
	}
	log("%s not found — installing via apt-get...", name)
	if out, err := exec.Command("apt-get", "install", "-y", "-q", name).CombinedOutput(); err != nil {
		return fmt.Errorf("installing %s: %w\n%s", name, err, string(out))
	}
	return nil
}

// ── Streaming download → decompress → disk ───────────────────────────────────

// streamImageToDisk downloads the image, optionally verifies its SHA-256, and
// writes the decompressed bytes to the disk device.  Decompression is done by
// an external process (zstd / xz) that reads from a Go io.Pipe, so only a
// small in-memory buffer is required — no large temp files are needed.
func streamImageToDisk(imageURL, imageHash, disk string) error {
	resp, err := http.Get(imageURL) //nolint:noctx
	if err != nil {
		return fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned HTTP %d", resp.StatusCode)
	}

	// Wrap body in a progress reporter.
	pr := &progressReader{r: resp.Body, total: resp.ContentLength, start: time.Now()}

	// Optionally tee into a SHA-256 hasher (computed on compressed bytes).
	var h hash.Hash
	var src io.Reader = pr
	if imageHash != "" {
		h = sha256.New()
		src = io.TeeReader(pr, h)
	}

	switch {
	case strings.HasSuffix(imageURL, ".zst"):
		err = runDecompressorStdoutToDisk(src, disk, "zstd", "-d", "-", "-c")
	case strings.HasSuffix(imageURL, ".xz"):
		// xz writes decompressed output to stdout; pipe that to the disk.
		err = runDecompressorStdoutToDisk(src, disk, "xz", "-d", "-c", "-")
	default:
		err = writeReaderToDisk(src, disk)
	}
	fmt.Println() // end progress line
	if err != nil {
		return err
	}

	if h != nil {
		actual := hex.EncodeToString(h.Sum(nil))
		if !strings.EqualFold(actual, imageHash) {
			return fmt.Errorf("SHA-256 mismatch\n  expected: %s\n  actual:   %s", imageHash, actual)
		}
		log("Hash verified OK.")
	}
	return nil
}

// runDecompressorToDisk runs a decompressor that writes its output directly to
// disk via a file-path argument (e.g. zstd -o <disk>).  src is connected to
// the process's stdin.
func runDecompressorToDisk(src io.Reader, disk, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdin = src
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// runDecompressorStdoutToDisk runs a decompressor whose decompressed output
// goes to stdout, then writes that stdout to the disk device.
func runDecompressorStdoutToDisk(src io.Reader, disk, name string, args ...string) error {
	piperd, pipewr := io.Pipe()

	cmd := exec.Command(name, args...)
	cmd.Stdin = src
	cmd.Stdout = pipewr
	cmd.Stderr = os.Stderr

	errCh := make(chan error, 1)
	go func() {
		runErr := cmd.Run()
		pipewr.CloseWithError(runErr)
		errCh <- runErr
	}()

	if err := writeReaderToDisk(piperd, disk); err != nil {
		return err
	}
	return <-errCh
}

// writeReaderToDisk copies r directly to the disk block device.
func writeReaderToDisk(r io.Reader, disk string) error {
	dst, err := os.OpenFile(disk, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("opening disk %s: %w", disk, err)
	}
	defer dst.Close()
	buf := make([]byte, ioBlockSize)
	if _, err := io.CopyBuffer(dst, r, buf); err != nil {
		return fmt.Errorf("writing to %s: %w", disk, err)
	}
	if err := dst.Sync(); err != nil {
		log("Warning: sync: %v", err)
	}
	return nil
}

// ── Config write ─────────────────────────────────────────────────────────────

// writeConfig mounts the Talos STATE partition and writes config.yaml.
// Talos GPT layout: EFI(1) BIOS(2) META(3) STATE(4) EPHEMERAL(5).
// Partition 6 is tried as a fallback for older layouts.
func writeConfig(disk string, config []byte) error {
	exec.Command("partprobe", disk).Run() //nolint:errcheck
	time.Sleep(2 * time.Second)

	var candidates []string
	if strings.Contains(disk, "nvme") || strings.Contains(disk, "mmcblk") {
		candidates = []string{disk + "p4", disk + "p6"}
	} else {
		candidates = []string{disk + "4", disk + "6"}
	}

	mountPoint, err := os.MkdirTemp("", "talos-state-*")
	if err != nil {
		return fmt.Errorf("creating mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)

	for _, part := range candidates {
		if _, err := os.Stat(part); err != nil {
			continue
		}
		if err := exec.Command("mount", part, mountPoint).Run(); err != nil {
			continue
		}
		configPath := filepath.Join(mountPoint, "config.yaml")
		writeErr := os.WriteFile(configPath, config, 0600)
		exec.Command("sync").Run()               //nolint:errcheck
		exec.Command("umount", mountPoint).Run() //nolint:errcheck
		if writeErr != nil {
			return fmt.Errorf("writing config to %s: %w", configPath, writeErr)
		}
		log("Machine config written to STATE partition (%s).", part)
		return nil
	}
	return fmt.Errorf("could not mount STATE partition (tried: %s)",
		strings.Join(candidates, ", "))
}

// ── Progress reader ──────────────────────────────────────────────────────────

type progressReader struct {
	r     io.Reader
	total int64
	read  int64
	start time.Time
}

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	if n > 0 {
		p.read += int64(n)
		elapsed := time.Since(p.start).Seconds()
		if elapsed < 0.01 {
			elapsed = 0.01
		}
		mbps := float64(p.read) / elapsed / (1024 * 1024)
		if p.total > 0 {
			pct := p.read * 100 / p.total
			fmt.Printf("\r  Download: %3d%%  %5d / %5d MiB  %.1f MiB/s    ",
				pct, p.read>>20, p.total>>20, mbps)
		} else {
			fmt.Printf("\r  Download: %5d MiB  %.1f MiB/s    ", p.read>>20, mbps)
		}
	}
	return n, err
}
