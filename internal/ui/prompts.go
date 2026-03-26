package ui

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/rothgar/k3s-to-talos/internal/k3s"
	"github.com/rothgar/k3s-to-talos/internal/talos"
)

var (
	bold    = color.New(color.Bold)
	red     = color.New(color.FgRed, color.Bold)
	yellow  = color.New(color.FgYellow, color.Bold)
	green   = color.New(color.FgGreen, color.Bold)
	cyan    = color.New(color.FgCyan)
	faint   = color.New(color.Faint)
)

// PrintPhaseHeader prints a prominent phase header.
func PrintPhaseHeader(num int, name, description string) {
	fmt.Println()
	cyan.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	bold.Printf("  Phase %d: %s\n", num, name)
	faint.Printf("  %s\n", description)
	cyan.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Println()
}

// PrintPhaseSkipped prints a message that a phase was skipped (resume mode).
func PrintPhaseSkipped(num int, name, reason string) {
	faint.Printf("\n  ↩  Phase %d: %s — skipped (%s)\n", num, name, reason)
}

// PrintClusterSummary prints a table of collected cluster information.
func PrintClusterSummary(info *k3s.ClusterInfo, backupDir string) {
	bold.Println("  Collected Cluster Information")
	fmt.Println()

	fmt.Printf("  %-22s %s\n", "k3s version:", info.K3sVersion)
	fmt.Printf("  %-22s %s\n", "Kubernetes version:", info.K8sVersion)
	fmt.Printf("  %-22s %s\n", "Datastore:", info.DatastoreType)
	if info.Hardware != nil {
		archDesc := info.Hardware.RawArch
		if info.Hardware.IsRaspberryPi {
			archDesc = fmt.Sprintf("%s (%s)", info.Hardware.RawArch, info.Hardware.PiModel)
		}
		fmt.Printf("  %-22s %s\n", "Architecture:", archDesc)
	}
	fmt.Printf("  %-22s %d\n", "Nodes:", len(info.Nodes))
	fmt.Printf("  %-22s %d\n", "Namespaces:", len(info.Namespaces))
	fmt.Printf("  %-22s %d\n", "Workloads:", info.WorkloadCount)
	fmt.Printf("  %-22s %d\n", "Persistent Volumes:", info.PVCount)
	fmt.Printf("  %-22s %s\n", "Backup directory:", backupDir)
	fmt.Println()

	if len(info.Nodes) > 0 {
		bold.Println("  Nodes:")
		fmt.Printf("    %-30s %-10s %-20s\n", "NAME", "STATUS", "ROLES")
		fmt.Printf("    %-30s %-10s %-20s\n",
			strings.Repeat("─", 29), strings.Repeat("─", 9), strings.Repeat("─", 19))
		for _, node := range info.Nodes {
			statusColor := color.New(color.FgGreen)
			if node.Status != "Ready" {
				statusColor = color.New(color.FgRed)
			}
			roles := node.Roles
			if roles == "" {
				roles = "<none>"
			}
			fmt.Printf("    %-30s %s %-20s\n",
				node.Name,
				statusColor.Sprintf("%-10s", node.Status),
				roles,
			)
		}
		fmt.Println()
	}

	if len(info.PVs) > 0 {
		yellow.Println("  Persistent Volumes (data NOT auto-migrated):")
		fmt.Printf("    %-30s %-10s %-20s %s\n", "NAME", "CAPACITY", "STORAGE CLASS", "BOUND TO")
		fmt.Printf("    %-30s %-10s %-20s %s\n",
			strings.Repeat("─", 29), strings.Repeat("─", 9), strings.Repeat("─", 19), strings.Repeat("─", 20))
		for _, pv := range info.PVs {
			claim := pv.ClaimRef
			if claim == "" {
				claim = "<unbound>"
			}
			fmt.Printf("    %-30s %-10s %-20s %s\n", pv.Name, pv.Capacity, pv.StorageClass, claim)
		}
		fmt.Println()

		yellow.Println("  ⚠ PV data will NOT be migrated automatically.")
		fmt.Println("    Back up your persistent data before proceeding.")
		fmt.Println("    See: https://velero.io or your storage backend's backup docs.")
		fmt.Println()
	}
}

// PrintMultiNodeWarning warns that multiple nodes exist and all must be migrated.
func PrintMultiNodeWarning(nodes []k3s.Node) {
	fmt.Println()
	yellow.Println("┌─────────────────────────────────────────────────────────────┐")
	yellow.Println("│              MULTI-NODE CLUSTER DETECTED                    │")
	yellow.Println("└─────────────────────────────────────────────────────────────┘")
	fmt.Println()
	fmt.Printf("  This cluster has %d nodes:\n\n", len(nodes))

	for _, node := range nodes {
		if node.IsControlPlane {
			fmt.Printf("    • %s  (control-plane) ← migrating this node now\n", node.Name)
		} else {
			fmt.Printf("    • %s  (worker)\n", node.Name)
		}
	}

	fmt.Println()
	yellow.Println("  IMPORTANT: Each node must be migrated individually.")
	fmt.Println("  Recommended order:")
	fmt.Println("  1. Migrate the control-plane node first (this run)")
	fmt.Println("  2. Drain each worker node before migrating it:")
	fmt.Println("       kubectl drain <node> --ignore-daemonsets --delete-emptydir-data")
	fmt.Println("  3. Run k3s-to-talos migrate on each worker with --talos-version")
	fmt.Println("     matching the control-plane version and the same --cluster-name")
	fmt.Println()
	yellow.Println("  Worker nodes will lose connectivity during their migration.")
	fmt.Println("  Ensure workloads can tolerate the temporary disruption.")
	fmt.Println()
}

// PrintRaspberryPiWarning prints a notice when the target is a Raspberry Pi 4/5
// that requires a custom image from factory.talos.dev.
func PrintRaspberryPiWarning(hw *talos.HardwareInfo) {
	if hw == nil || !hw.IsRaspberryPi {
		return
	}
	fmt.Println()
	yellow.Println("┌─────────────────────────────────────────────────────────────┐")
	yellow.Printf( "│  Raspberry Pi %d detected: %s\n", hw.PiGen,
		padRight(hw.PiModel, 35)+"│")
	yellow.Println("├─────────────────────────────────────────────────────────────┤")
	yellow.Println("│  Talos Linux requires a CUSTOM IMAGE for Raspberry Pi 4/5.  │")
	yellow.Println("│                                                              │")
	yellow.Println("│  A schematic with the 'rpi_generic' overlay will be         │")
	yellow.Println("│  submitted to factory.talos.dev to generate the correct     │")
	yellow.Println("│  arm64 image. This requires outbound internet access from   │")
	yellow.Println("│  the machine running k3s-to-talos.                          │")
	yellow.Println("│                                                              │")
	yellow.Println("│  Standard amd64 images will NOT boot on Raspberry Pi.       │")
	yellow.Println("└─────────────────────────────────────────────────────────────┘")
	fmt.Println()
}

// padRight pads or truncates s to exactly n characters.
func padRight(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	return s + strings.Repeat(" ", n-len(s))
}

// PrintIrreversibilityWarning prints a prominent red warning about the irreversible nature.
func PrintIrreversibilityWarning(host string) {
	fmt.Println()
	red.Println("┌──────────────────────────────────────────────────────────────┐")
	red.Println("│           !! WARNING: THIS ACTION IS IRREVERSIBLE !!         │")
	red.Println("├──────────────────────────────────────────────────────────────┤")
	red.Printf( "│  Target machine: %-43s│\n", host)
	red.Println("│                                                              │")
	red.Println("│  • The machine's OS disk will be COMPLETELY ERASED.          │")
	red.Println("│  • All data NOT already backed up will be PERMANENTLY LOST.  │")
	red.Println("│  • Talos Linux will replace the entire operating system.     │")
	red.Println("│  • This cannot be undone without reinstalling from scratch.  │")
	red.Println("│                                                              │")
	red.Println("│  k3s-to-talos has saved a backup to your --backup-dir.      │")
	red.Println("│  Verify your backup is complete before proceeding.           │")
	red.Println("└──────────────────────────────────────────────────────────────┘")
	fmt.Println()
}

// ConfirmErase prompts the user to type an exact confirmation string before proceeding.
func ConfirmErase(host string) error {
	required := fmt.Sprintf("I understand this will ERASE %s", host)

	fmt.Printf("To proceed, type exactly:\n\n")
	bold.Printf("    %s\n\n", required)
	fmt.Printf("Confirmation: ")

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading confirmation: %w", err)
	}
	input = strings.TrimRight(input, "\r\n")

	if input != required {
		return fmt.Errorf("confirmation did not match — migration aborted\nYou typed: %q\nExpected:   %q", input, required)
	}

	green.Println("\n  Confirmation accepted. Proceeding with migration.")
	return nil
}
