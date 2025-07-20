package main

import (
	"fmt"
	"runtime/debug"
)

func getVersion(version string) string {
	var (
		commitDate string
		commit     string
	)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				commit = setting.Value
			}
			if setting.Key == "vcs.time" {
				commitDate = setting.Value
			}
		}
	}
	if version != "" {
		return fmt.Sprintf("%s %s %s", version, commit, commitDate)
	}
	return fmt.Sprintf("%s %s", commit, commitDate)
}
