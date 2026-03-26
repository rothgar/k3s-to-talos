package talos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Architecture constants.
const (
	ArchAMD64 = "amd64"
	ArchARM64 = "arm64"
)

// PiGeneration represents a detected Raspberry Pi generation.
type PiGeneration int

const (
	PiUnknown PiGeneration = 0
	Pi3       PiGeneration = 3
	Pi4       PiGeneration = 4
	Pi5       PiGeneration = 5
)

// HardwareInfo describes the target machine's hardware.
type HardwareInfo struct {
	// Arch is the normalised architecture: "amd64" or "arm64".
	Arch string `json:"arch"`
	// RawArch is the exact uname -m output (e.g. "x86_64", "aarch64").
	RawArch string `json:"raw_arch"`
	// IsRaspberryPi is true when /sys/firmware/devicetree/base/model contains "Raspberry Pi".
	IsRaspberryPi bool `json:"is_raspberry_pi"`
	// PiModel is the full model string, e.g. "Raspberry Pi 4 Model B Rev 1.4".
	PiModel string `json:"pi_model,omitempty"`
	// PiGen is the detected generation (3, 4, 5, or 0 if not a Pi).
	PiGen PiGeneration `json:"pi_gen,omitempty"`
	// ImageFactorySchematicID is the factory.talos.dev schematic ID for this board.
	// Only set for boards that require a custom overlay.
	ImageFactorySchematicID string `json:"image_factory_schematic_id,omitempty"`
}

// NeedsImageFactory returns true if this hardware requires a custom image from
// factory.talos.dev rather than the standard GitHub release binary.
func (h *HardwareInfo) NeedsImageFactory() bool {
	return h.IsRaspberryPi && (h.PiGen == Pi4 || h.PiGen == Pi5)
}

// Supported returns false when the hardware is known to be unsupported by Talos.
func (h *HardwareInfo) Supported() error {
	if h.IsRaspberryPi && h.PiGen == Pi3 {
		return fmt.Errorf(
			"Raspberry Pi 3 is NOT supported by Talos Linux.\n\n"+
				"Talos requires 64-bit ARM (aarch64) with u-boot arm64 support.\n"+
				"The Raspberry Pi 3 lacks the necessary bootloader support in the\n"+
				"Talos arm64 image.\n\n"+
				"Supported ARM boards: Raspberry Pi 4, Raspberry Pi 5, and other\n"+
				"aarch64 SBCs with appropriate overlays from factory.talos.dev.\n"+
				"See: https://www.talos.dev/latest/talos-guides/install/single-board-computers/",
		)
	}
	if h.Arch != ArchAMD64 && h.Arch != ArchARM64 {
		return fmt.Errorf(
			"unsupported architecture %q — Talos Linux supports amd64 and arm64 only",
			h.RawArch,
		)
	}
	return nil
}

// SSHRunner is a minimal interface for running remote commands, satisfied by *ssh.Client.
type SSHRunner interface {
	Run(cmd string) (string, error)
}

// DetectHardware inspects the remote machine to determine its CPU architecture
// and whether it is a Raspberry Pi (and which generation).
func DetectHardware(runner SSHRunner) (*HardwareInfo, error) {
	hw := &HardwareInfo{}

	// 1. Detect CPU architecture via uname -m.
	rawArch, err := runner.Run("uname -m")
	if err != nil {
		return nil, fmt.Errorf("detecting architecture: %w", err)
	}
	hw.RawArch = strings.TrimSpace(rawArch)
	switch hw.RawArch {
	case "x86_64":
		hw.Arch = ArchAMD64
	case "aarch64", "arm64":
		hw.Arch = ArchARM64
	default:
		hw.Arch = hw.RawArch
	}

	// 2. On ARM, check for Raspberry Pi via the device-tree model node.
	if hw.Arch == ArchARM64 {
		detectRaspberryPi(runner, hw)
	}

	return hw, nil
}

// detectRaspberryPi reads the device-tree model string to identify Pi generation.
func detectRaspberryPi(runner SSHRunner, hw *HardwareInfo) {
	// Strip the null byte that is appended by the kernel to device-tree strings.
	model, err := runner.Run("cat /sys/firmware/devicetree/base/model 2>/dev/null | tr -d '\\0'")
	if err != nil || strings.TrimSpace(model) == "" {
		return
	}
	model = strings.TrimSpace(model)

	if !strings.Contains(model, "Raspberry Pi") {
		return
	}

	hw.IsRaspberryPi = true
	hw.PiModel = model

	// Determine generation from the model string.
	switch {
	case strings.Contains(model, "Raspberry Pi 5"):
		hw.PiGen = Pi5
	case strings.Contains(model, "Raspberry Pi 4"):
		hw.PiGen = Pi4
	case strings.Contains(model, "Raspberry Pi 3"):
		hw.PiGen = Pi3
	case strings.Contains(model, "Raspberry Pi 2"):
		hw.PiGen = Pi3 // Treat Pi 2 same as Pi 3: unsupported
	default:
		// Pi 1, Zero, etc.
		hw.PiGen = Pi3
	}
}

// rpiSchematic is the YAML posted to factory.talos.dev for Raspberry Pi 4/5.
// The rpi_generic overlay works for both Pi 4 and Pi 5.
const rpiSchematic = `overlays:
  - name: rpi_generic
    image: siderolabs/sbc-raspberrypi
`

// defaultSchematic is the YAML posted to factory.talos.dev for generic x86_64/arm64
// machines that require no custom overlays or extensions.
const defaultSchematic = `customization: {}
`

type factoryResponse struct {
	ID string `json:"id"`
}

// GetImageFactorySchematicID submits the given schematic YAML to factory.talos.dev
// and returns the content-addressable schematic ID.
func GetImageFactorySchematicID(schematicYAML string) (string, error) {
	client := &http.Client{Timeout: 15 * time.Second}

	req, err := http.NewRequest(http.MethodPost, "https://factory.talos.dev/schematics",
		bytes.NewBufferString(schematicYAML))
	if err != nil {
		return "", fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/yaml")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("posting schematic: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("factory API returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading factory response: %w", err)
	}

	var r factoryResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("parsing factory response: %w", err)
	}
	if r.ID == "" {
		return "", fmt.Errorf("factory API returned empty schematic ID")
	}
	return r.ID, nil
}

// ResolveImageURL returns the correct Talos metal disk image URL for the
// target hardware and version. Always uses factory.talos.dev so the URL
// format is consistent across all Talos versions.
func ResolveImageURL(version string, hw *HardwareInfo) (imageURL, hash string, err error) {
	return resolveFactoryImageURL(version, hw)
}

func resolveFactoryImageURL(version string, hw *HardwareInfo) (imageURL, hash string, err error) {
	// Get (or reuse cached) schematic ID.
	schematicID := hw.ImageFactorySchematicID
	if schematicID == "" {
		schematic := defaultSchematic
		if hw.IsRaspberryPi && (hw.PiGen == Pi4 || hw.PiGen == Pi5) {
			schematic = rpiSchematic
		}
		schematicID, err = GetImageFactorySchematicID(schematic)
		if err != nil {
			return "", "", fmt.Errorf("getting image factory schematic: %w", err)
		}
		hw.ImageFactorySchematicID = schematicID
	}

	arch := hw.Arch
	if arch == "" {
		arch = ArchAMD64
	}
	imageURL = fmt.Sprintf(
		"https://factory.talos.dev/image/%s/%s/metal-%s.raw.zst",
		schematicID, version, arch,
	)
	// factory.talos.dev does not provide a separate checksum file; skip hash verification.
	return imageURL, "", nil
}

