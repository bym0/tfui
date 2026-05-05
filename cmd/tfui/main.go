package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	tea "charm.land/bubbletea/v2"
	"charm.land/log/v2"
	"github.com/SayYoungMan/tfui/internal/ui"
	"github.com/SayYoungMan/tfui/pkg/terraform"
)

func main() {
	binary := flag.String("binary", "terraform", "path or name of the terraform binary")
	workdir := flag.String("dir", "", "directory to find resources (defaults to current directory)")
	flag.Parse()

	closeLog := setUpLogging()
	defer closeLog()

	if *workdir == "" {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error occurred during start up: %v\n", err)
			os.Exit(1)
		}
		*workdir = wd
	}

	binarySet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "binary" {
			binarySet = true
		}
	})

	stackMode := false
	if !binarySet && isTerragruntProject(*workdir) {
		*binary = "terragrunt"
		stackMode = isTerragruntStack(*workdir)
		log.Info("detected terragrunt project", "stack", stackMode)
	}

	if _, err := exec.LookPath(*binary); err != nil {
		fmt.Fprintf(os.Stderr, "%q not found in PATH\n", *binary)
		os.Exit(1)
	}

	if stackMode {
		fmt.Fprint(os.Stderr, "generating terragrunt stack...\n")
		gen := exec.Command(*binary, "stack", "generate")
		gen.Dir = *workdir
		gen.Stderr = os.Stderr
		if err := gen.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "terragrunt stack generate failed: %v\n", err)
			os.Exit(1)
		}
		*workdir = filepath.Join(*workdir, ".terragrunt-stack")
	}

	log.Info("starting tfui", "binary", *binary, "workdir", *workdir)

	runner := terraform.NewTerraformRunner(*workdir, *binary)
	runner.SetStackMode(stackMode)

	m := ui.NewModel(runner)
	p := tea.NewProgram(m)
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error occurred while running program: %v\n", err)
		os.Exit(1)
	}
}

func isTerragruntStack(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "terragrunt.stack.hcl"))
	return err == nil
}

func isTerragruntProject(dir string) bool {
	if isTerragruntStack(dir) {
		return true
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*terragrunt*.hcl"))
	return len(matches) > 0
}

func setUpLogging() func() {
	if os.Getenv("TFUI_DEBUG") != "1" {
		log.SetOutput(io.Discard)
		return func() {}
	}

	f, err := os.OpenFile("tfui-debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open debug log: %v\n", err)
		log.SetOutput(io.Discard)
		return func() {}
	}

	log.SetOutput(f)
	log.SetLevel(log.DebugLevel)
	log.SetReportTimestamp(true)
	log.SetReportCaller(true)
	log.SetTimeFormat("15:04:05.000")

	log.Info("=== tfui debug log started ===")

	return func() { f.Close() }
}
