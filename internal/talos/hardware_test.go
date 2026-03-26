package talos

import (
	"strings"
	"testing"
)

// ── detectRaspberryPi ─────────────────────────────────────────────────────────

// fakeRunner lets us simulate SSH command output without a real connection.
type fakeRunner struct {
	output string
	err    error
}

func (f *fakeRunner) Run(_ string) (string, error) {
	return f.output, f.err
}

func TestDetectRaspberryPi_Pi4(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchARM64}
	r := &fakeRunner{output: "Raspberry Pi 4 Model B Rev 1.4"}
	detectRaspberryPi(r, hw)

	if !hw.IsRaspberryPi {
		t.Fatal("expected IsRaspberryPi=true")
	}
	if hw.PiGen != Pi4 {
		t.Errorf("expected Pi4 (%d), got %d", Pi4, hw.PiGen)
	}
	if hw.PiModel != "Raspberry Pi 4 Model B Rev 1.4" {
		t.Errorf("unexpected PiModel: %q", hw.PiModel)
	}
}

func TestDetectRaspberryPi_Pi5(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchARM64}
	r := &fakeRunner{output: "Raspberry Pi 5 Model B Rev 1.0"}
	detectRaspberryPi(r, hw)

	if hw.PiGen != Pi5 {
		t.Errorf("expected Pi5 (%d), got %d", Pi5, hw.PiGen)
	}
}

func TestDetectRaspberryPi_Pi3(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchARM64}
	r := &fakeRunner{output: "Raspberry Pi 3 Model B Plus Rev 1.3"}
	detectRaspberryPi(r, hw)

	if hw.PiGen != Pi3 {
		t.Errorf("expected Pi3 (%d), got %d", Pi3, hw.PiGen)
	}
}

func TestDetectRaspberryPi_NotPi(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchARM64}
	// Generic ARM board — device-tree model does not contain "Raspberry Pi"
	r := &fakeRunner{output: "Rockchip RK3588 EVB Board"}
	detectRaspberryPi(r, hw)

	if hw.IsRaspberryPi {
		t.Error("expected IsRaspberryPi=false for a non-Pi board")
	}
}

func TestDetectRaspberryPi_EmptyModel(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchARM64}
	r := &fakeRunner{output: ""}
	detectRaspberryPi(r, hw)

	if hw.IsRaspberryPi {
		t.Error("expected IsRaspberryPi=false when model string is empty")
	}
}

// ── DetectHardware ────────────────────────────────────────────────────────────

func TestDetectHardware_AMD64(t *testing.T) {
	r := &fakeRunner{output: "x86_64"}
	hw, err := DetectHardware(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hw.Arch != ArchAMD64 {
		t.Errorf("expected amd64, got %q", hw.Arch)
	}
	if hw.IsRaspberryPi {
		t.Error("x86_64 should not be a Pi")
	}
}

// multiCmdRunner returns different outputs for successive calls.
type multiCmdRunner struct {
	outputs []string
	idx     int
}

func (m *multiCmdRunner) Run(_ string) (string, error) {
	if m.idx >= len(m.outputs) {
		return "", nil
	}
	out := m.outputs[m.idx]
	m.idx++
	return out, nil
}

func TestDetectHardware_ARM64Pi4(t *testing.T) {
	r := &multiCmdRunner{
		outputs: []string{
			"aarch64",                         // uname -m
			"Raspberry Pi 4 Model B Rev 1.4",  // cat /sys/firmware/devicetree/base/model
		},
	}
	hw, err := DetectHardware(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hw.Arch != ArchARM64 {
		t.Errorf("expected arm64, got %q", hw.Arch)
	}
	if !hw.IsRaspberryPi || hw.PiGen != Pi4 {
		t.Errorf("expected Pi4, got IsRaspberryPi=%v PiGen=%d", hw.IsRaspberryPi, hw.PiGen)
	}
}

// ── HardwareInfo.Supported ────────────────────────────────────────────────────

func TestSupported_Pi3Fails(t *testing.T) {
	hw := &HardwareInfo{
		Arch:          ArchARM64,
		IsRaspberryPi: true,
		PiGen:         Pi3,
	}
	err := hw.Supported()
	if err == nil {
		t.Fatal("expected error for Pi3, got nil")
	}
	if !strings.Contains(err.Error(), "NOT supported") {
		t.Errorf("error should mention 'NOT supported', got: %v", err)
	}
}

func TestSupported_Pi4OK(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchARM64, IsRaspberryPi: true, PiGen: Pi4}
	if err := hw.Supported(); err != nil {
		t.Errorf("Pi4 should be supported, got: %v", err)
	}
}

func TestSupported_Pi5OK(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchARM64, IsRaspberryPi: true, PiGen: Pi5}
	if err := hw.Supported(); err != nil {
		t.Errorf("Pi5 should be supported, got: %v", err)
	}
}

func TestSupported_AMD64OK(t *testing.T) {
	hw := &HardwareInfo{Arch: ArchAMD64}
	if err := hw.Supported(); err != nil {
		t.Errorf("amd64 should be supported, got: %v", err)
	}
}

func TestSupported_ARMv7Fails(t *testing.T) {
	hw := &HardwareInfo{Arch: "armv7l", RawArch: "armv7l"}
	err := hw.Supported()
	if err == nil {
		t.Fatal("expected error for armv7l, got nil")
	}
}

// ── NeedsImageFactory ─────────────────────────────────────────────────────────

func TestNeedsImageFactory(t *testing.T) {
	cases := []struct {
		hw   HardwareInfo
		want bool
	}{
		{HardwareInfo{Arch: ArchAMD64}, false},
		{HardwareInfo{Arch: ArchARM64, IsRaspberryPi: false}, false},
		{HardwareInfo{Arch: ArchARM64, IsRaspberryPi: true, PiGen: Pi3}, false},
		{HardwareInfo{Arch: ArchARM64, IsRaspberryPi: true, PiGen: Pi4}, true},
		{HardwareInfo{Arch: ArchARM64, IsRaspberryPi: true, PiGen: Pi5}, true},
	}
	for _, c := range cases {
		got := c.hw.NeedsImageFactory()
		if got != c.want {
			t.Errorf("NeedsImageFactory(%+v) = %v, want %v", c.hw, got, c.want)
		}
	}
}

