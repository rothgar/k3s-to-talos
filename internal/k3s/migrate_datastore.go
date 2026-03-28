package k3s

import (
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/rothgar/k3s-to-talos/internal/ssh"
)

// MigrateToEtcd converts a k3s server running SQLite to embedded etcd using
// the k3s --cluster-init mechanism.  After the migration, k3s restarts with
// embedded etcd so the caller can take a proper etcd snapshot.
//
// The migration works as follows:
//  1. Read the existing k3s systemd service dropin / config to find ExecStart
//  2. Add --cluster-init to the k3s ExecStart arguments
//  3. Reload systemd and restart k3s
//  4. Poll until the etcd/member directory appears (up to 5 min)
//  5. Remove the --cluster-init flag so subsequent restarts are clean
//  6. Restart k3s one more time to make the change permanent
func MigrateToEtcd(sshClient *ssh.Client) error {
	color.Blue("  Converting k3s SQLite datastore to embedded etcd...\n")

	// ── Step 1: determine the current ExecStart line ────────────────────────
	// k3s ships with /lib/systemd/system/k3s.service (or /etc/systemd/system/k3s.service).
	// We add a dropin that appends --cluster-init to the ExecStart.
	const dropinPath = "/etc/systemd/system/k3s.service.d/migrate-to-etcd.conf"

	// Check which service file is present.
	svcFile := "/lib/systemd/system/k3s.service"
	if exists := sshClient.FileExists("/etc/systemd/system/k3s.service"); exists {
		svcFile = "/etc/systemd/system/k3s.service"
	}

	// Read the current ExecStart to extract the binary path and existing args so
	// we can build the dropin.
	svcContent, err := sshClient.Run(fmt.Sprintf("cat %s 2>/dev/null", svcFile))
	if err != nil {
		return fmt.Errorf("reading k3s service file: %w", err)
	}

	// Find the ExecStart line.
	var execStart string
	for _, line := range strings.Split(svcContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "ExecStart=") {
			execStart = trimmed
			break
		}
	}
	if execStart == "" {
		return fmt.Errorf("could not find ExecStart in %s", svcFile)
	}

	// If --cluster-init is already present the migration was done previously.
	if strings.Contains(execStart, "--cluster-init") {
		color.Yellow("  k3s already has --cluster-init in ExecStart — checking etcd member dir\n")
	} else {
		// ── Step 2: write the dropin ───────────────────────────────────────────
		// Systemd dropin: override ExecStart= requires clearing it first (empty
		// line), then providing the new value.  We append --cluster-init.
		newExecStart := execStart + " \\\n    --cluster-init"
		dropin := fmt.Sprintf("[Service]\nExecStart=\n%s\n", newExecStart)

		if _, err := sshClient.Run("mkdir -p /etc/systemd/system/k3s.service.d"); err != nil {
			return fmt.Errorf("creating systemd drop-in directory: %w", err)
		}

		writeCmd := fmt.Sprintf("printf '%%s' %s > %s",
			shellescape(dropin), dropinPath)
		if _, err := sshClient.Run(writeCmd); err != nil {
			// printf shell-escape may be fragile; fall back to tee with heredoc.
			heredoc := fmt.Sprintf("cat > %s <<'DROPIN_EOF'\n%sDROPIN_EOF", dropinPath, dropin)
			if _, err2 := sshClient.Run(heredoc); err2 != nil {
				return fmt.Errorf("writing systemd drop-in: %w (fallback: %v)", err, err2)
			}
		}

		// ── Step 3: reload systemd and restart k3s ────────────────────────────
		if _, err := sshClient.Run("systemctl daemon-reload"); err != nil {
			return fmt.Errorf("systemctl daemon-reload: %w", err)
		}
		color.Blue("  Restarting k3s with --cluster-init (this migrates SQLite → etcd)...\n")
		if _, err := sshClient.Run("systemctl restart k3s"); err != nil {
			return fmt.Errorf("restarting k3s: %w", err)
		}
	}

	// ── Step 4: poll for etcd/member ──────────────────────────────────────
	color.Blue("  Waiting for embedded etcd to initialise (up to 5 min)...\n")
	deadline := time.Now().Add(5 * time.Minute)
	pollInterval := 5 * time.Second
	for time.Now().Before(deadline) {
		if sshClient.FileExists("/var/lib/rancher/k3s/server/db/etcd/member") {
			color.Green("  ✓ Embedded etcd is running\n")
			break
		}
		time.Sleep(pollInterval)
	}
	if !sshClient.FileExists("/var/lib/rancher/k3s/server/db/etcd/member") {
		return fmt.Errorf(
			"timed out waiting for embedded etcd (etcd/member directory not found after 5 min);\n" +
				"check k3s logs: journalctl -u k3s -n 100")
	}

	// ── Step 5: remove the --cluster-init dropin ──────────────────────────
	// k3s docs state that after the initial migration the flag must be
	// removed so k3s doesn't try to reinitialise on every restart.
	if sshClient.FileExists(dropinPath) {
		if _, err := sshClient.Run(fmt.Sprintf("rm -f %s", dropinPath)); err != nil {
			color.Yellow("  Warning: could not remove migration drop-in %s: %v\n", dropinPath, err)
			color.Yellow("  You should remove it manually before rebooting.\n")
		}
		if _, err := sshClient.Run("systemctl daemon-reload"); err != nil {
			color.Yellow("  Warning: systemctl daemon-reload after drop-in removal failed: %v\n", err)
		}
	}

	// ── Step 6: restart k3s without --cluster-init ────────────────────────
	color.Blue("  Restarting k3s without --cluster-init to verify clean startup...\n")
	if _, err := sshClient.Run("systemctl restart k3s"); err != nil {
		return fmt.Errorf("restarting k3s after removing --cluster-init: %w", err)
	}

	// Brief wait for k3s to stabilise.
	time.Sleep(10 * time.Second)

	// Final check: etcd/member must still be present.
	if !sshClient.FileExists("/var/lib/rancher/k3s/server/db/etcd/member") {
		return fmt.Errorf("etcd/member directory disappeared after clean restart; k3s may have fallen back to SQLite")
	}

	color.Green("  ✓ k3s is now running with embedded etcd\n")
	return nil
}

// shellescape wraps s in single quotes, escaping any embedded single quotes.
// Used to safely pass multi-line strings to the remote shell via printf.
func shellescape(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
