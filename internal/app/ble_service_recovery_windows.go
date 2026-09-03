//go:build windows

package app

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"syscall"
	"time"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

const shellExecuteMaskNoCloseProcess = 0x00000040

var shellExecuteExW = windows.NewLazySystemDLL("shell32.dll").NewProc("ShellExecuteExW")

// shellExecuteInfo mirrors SHELLEXECUTEINFOW. Keeping the process handle lets
// the server wait until the elevated recovery has actually completed.
type shellExecuteInfo struct {
	Size       uint32
	Mask       uint32
	Window     windows.Handle
	Verb       *uint16
	File       *uint16
	Parameters *uint16
	Directory  *uint16
	Show       int32
	Instance   windows.Handle
	IDList     unsafe.Pointer
	Class      *uint16
	ClassKey   windows.Handle
	HotKey     uint32
	Icon       windows.Handle
	Process    windows.Handle
}

func recoverBluetoothServicesElevated(ctx context.Context) error {
	windowsDirectory, err := windows.GetSystemWindowsDirectory()
	if err != nil {
		return fmt.Errorf("locate Windows PowerShell: %w", err)
	}
	powerShell := filepath.Join(windowsDirectory, "System32", "WindowsPowerShell", "v1.0", "powershell.exe")
	command := `$ErrorActionPreference='Stop'; ` +
		`$rtk=Get-Service -Name 'RtkBtManServ' -ErrorAction SilentlyContinue; ` +
		`if ($null -ne $rtk) { if ($rtk.Status -eq 'Stopped') { Start-Service -Name 'RtkBtManServ' } else { Restart-Service -Name 'RtkBtManServ' -Force } }; ` +
		`$bt=Get-Service -Name 'bthserv' -ErrorAction Stop; ` +
		`if ($bt.Status -eq 'Stopped') { Start-Service -Name 'bthserv' } else { Restart-Service -Name 'bthserv' -Force }; ` +
		`Start-Sleep -Seconds 5`
	parameters := "-NoProfile -NonInteractive -EncodedCommand " + encodePowerShellCommand(command)
	return runElevatedAndWait(ctx, powerShell, parameters)
}

func encodePowerShellCommand(command string) string {
	encoded := utf16.Encode([]rune(command))
	bytes := make([]byte, len(encoded)*2)
	for i, value := range encoded {
		binary.LittleEndian.PutUint16(bytes[i*2:], value)
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

func runElevatedAndWait(ctx context.Context, executable, parameters string) error {
	verb, err := windows.UTF16PtrFromString("runas")
	if err != nil {
		return err
	}
	file, err := windows.UTF16PtrFromString(executable)
	if err != nil {
		return err
	}
	arguments, err := windows.UTF16PtrFromString(parameters)
	if err != nil {
		return err
	}
	info := shellExecuteInfo{
		Mask:       shellExecuteMaskNoCloseProcess,
		Verb:       verb,
		File:       file,
		Parameters: arguments,
		Show:       windows.SW_HIDE,
	}
	info.Size = uint32(unsafe.Sizeof(info))
	result, _, callErr := shellExecuteExW.Call(uintptr(unsafe.Pointer(&info)))
	if result == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return fmt.Errorf("request administrator permission: %w", callErr)
		}
		return fmt.Errorf("request administrator permission: ShellExecuteExW failed")
	}
	if info.Process == 0 {
		return fmt.Errorf("elevated recovery returned no process handle")
	}
	defer windows.CloseHandle(info.Process)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		waitResult, err := windows.WaitForSingleObject(info.Process, 250)
		if err != nil {
			return fmt.Errorf("wait for elevated recovery: %w", err)
		}
		if waitResult == windows.WAIT_OBJECT_0 {
			var exitCode uint32
			if err := windows.GetExitCodeProcess(info.Process, &exitCode); err != nil {
				return fmt.Errorf("read elevated recovery result: %w", err)
			}
			if exitCode != 0 {
				return fmt.Errorf("elevated recovery exited with code %d", exitCode)
			}
			return nil
		}
		if waitResult != uint32(windows.WAIT_TIMEOUT) {
			return fmt.Errorf("wait for elevated recovery returned status 0x%x", waitResult)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
