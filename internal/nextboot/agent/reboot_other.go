//go:build !linux

package agent

import "fmt"

func reboot() error {
	fmt.Println("[nextboot] Auto-reboot is only supported on Linux.")
	fmt.Println("[nextboot] Reboot the machine manually to boot into Talos.")
	return nil
}
