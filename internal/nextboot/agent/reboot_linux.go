//go:build linux

package agent

import "syscall"

func reboot() error {
	return syscall.Reboot(syscall.LINUX_REBOOT_CMD_RESTART)
}
