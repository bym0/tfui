package terraform

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"charm.land/log/v2"
)

const MB = 1024 * 1024

// Provides ability to override exec.CommandContext for mock testing
type CommandFactory func(ctx context.Context, name string, args ...string) *exec.Cmd

type TerraformRunner struct {
	binary     string
	workdir    string
	stackMode  bool
	cmdFactory CommandFactory
}

func NewTerraformRunner(workdir string, binary string) *TerraformRunner {
	if binary == "" {
		binary = "terraform"
	}
	return &TerraformRunner{
		binary:     binary,
		workdir:    workdir,
		cmdFactory: exec.CommandContext,
	}
}

func (tr *TerraformRunner) SetStackMode(enabled bool) {
	tr.stackMode = enabled
}

func (tr *TerraformRunner) configureStackEnv(cmd *exec.Cmd) {
	cmd.Env = append(os.Environ(), cmd.Env...)

	setIfUnset := func(key, value string) {
		if os.Getenv(key) == "" {
			cmd.Env = append(cmd.Env, key+"="+value)
			log.Debug("set env for stack command", "key", key, "value", value)
		}
	}

	home, err := os.UserHomeDir()
	if err == nil {
		cacheDir := filepath.Join(home, ".terraform.d", "plugin-cache")
		os.MkdirAll(cacheDir, 0o755)
		setIfUnset("TF_PLUGIN_CACHE_DIR", cacheDir)
	}

	setIfUnset("TG_PARALLELISM", strconv.Itoa(runtime.NumCPU()))
}

func (tr *TerraformRunner) stackPrefix(args []string) []string {
	if !tr.stackMode {
		return args
	}
	return append([]string{"stack", "run"}, args...)
}

func (tr *TerraformRunner) Plan(ctx context.Context, targets []string) <-chan StreamEvent {
	if tr.stackMode {
		return tr.stackPlan(ctx, targets)
	}
	args := []string{"plan", "-json"}
	for _, t := range targets {
		args = append(args, fmt.Sprintf("-target=%s", t))
	}
	return tr.streamJsonEvents(ctx, args)
}

func (tr *TerraformRunner) stackPlan(ctx context.Context, targets []string) <-chan StreamEvent {
	ch := make(chan StreamEvent)

	go func() {
		defer close(ch)

		tmpDir, err := os.MkdirTemp("", "tfui-plan-*")
		if err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to create temp dir: %w", err)}
			return
		}
		defer os.RemoveAll(tmpDir)

		args := []string{"stack", "run", "plan", "--json-out-dir", tmpDir}

		cmd := tr.cmdFactory(ctx, tr.binary, args...)
		cmd.Dir = tr.workdir
		tr.configureStackEnv(cmd)
		cmd.Cancel = func() error {
			return cmd.Process.Signal(os.Interrupt)
		}

		stdoutPipe, err := cmd.StdoutPipe()
		if err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to pipe stdout: %w", err)}
			return
		}

		stderrPipe, err := cmd.StderrPipe()
		if err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to pipe stderr: %w", err)}
			return
		}

		if err := cmd.Start(); err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("terragrunt stack run plan failed to start: %w", err)}
			return
		}

		var mu sync.Mutex
		var outputLines []string
		var wg sync.WaitGroup

		appendAndSend := func(line string) {
			mu.Lock()
			outputLines = append(outputLines, line)
			mu.Unlock()
			ch <- StreamEvent{Message: line}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s := bufio.NewScanner(stdoutPipe)
			for s.Scan() {
				line := strings.TrimSpace(s.Text())
				if line != "" {
					appendAndSend(line)
				}
			}
		}()

		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				appendAndSend(line)
			}
		}

		wg.Wait()
		cmdErr := cmd.Wait()
		if cmdErr != nil {
			log.Debug("terragrunt stack run plan", "exit_error", cmdErr)
		}

		events, err := parsePlanDir(tmpDir)
		if err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to parse plan output: %w", err)}
			return
		}

		if len(events) == 0 && cmdErr != nil {
			tail := outputLines
			if len(tail) > 20 {
				tail = tail[len(tail)-20:]
			}
			ch <- StreamEvent{Error: fmt.Errorf("terragrunt stack run plan failed: %w\n%s", cmdErr, strings.Join(tail, "\n"))}
			return
		}

		for _, e := range events {
			ch <- e
		}
	}()

	return ch
}

func (tr *TerraformRunner) Apply(ctx context.Context, targets []string) <-chan StreamEvent {
	if tr.stackMode {
		args := []string{"stack", "run", "apply", "--non-interactive"}
		return tr.stackStreamJsonEvents(ctx, args)
	}
	args := []string{"apply", "-auto-approve", "-json"}
	for _, t := range targets {
		args = append(args, fmt.Sprintf("-target=%s", t))
	}
	return tr.streamJsonEvents(ctx, args)
}

func (tr *TerraformRunner) Destroy(ctx context.Context, targets []string) <-chan StreamEvent {
	if tr.stackMode {
		args := []string{"stack", "run", "destroy", "--non-interactive"}
		return tr.stackStreamJsonEvents(ctx, args)
	}
	args := []string{"destroy", "-auto-approve", "-json"}
	for _, t := range targets {
		args = append(args, fmt.Sprintf("-target=%s", t))
	}
	return tr.streamJsonEvents(ctx, args)
}

func (tr *TerraformRunner) stackStreamJsonEvents(ctx context.Context, args []string) <-chan StreamEvent {
	ch := make(chan StreamEvent)

	go func() {
		defer close(ch)

		cmd := tr.cmdFactory(ctx, tr.binary, args...)
		cmd.Dir = tr.workdir
		tr.configureStackEnv(cmd)
		cmd.Cancel = func() error {
			return cmd.Process.Signal(os.Interrupt)
		}

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to pipe stdout: %w", err)}
			return
		}

		stderrPipe, err := cmd.StderrPipe()
		if err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to pipe stderr: %w", err)}
			return
		}

		if err := cmd.Start(); err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("command failed to start: %w", err)}
			return
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			scanner := bufio.NewScanner(stderrPipe)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}
				if textEvent := ParseTextLine(line); textEvent != nil {
					ch <- *textEvent
				} else {
					ch <- StreamEvent{Message: line}
				}
			}
		}()

		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, MB), MB)
		for scanner.Scan() {
			event := ParseLine(scanner.Bytes())
			if event != nil {
				ch <- *event
				continue
			}
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if textEvent := ParseTextLine(line); textEvent != nil {
				ch <- *textEvent
			} else {
				ch <- StreamEvent{Message: line}
			}
		}
		if err := scanner.Err(); err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("scanner error: %w", err)}
		}

		wg.Wait()

		if err := cmd.Wait(); err != nil {
			log.Debug("stack command exited with error", "error", err)
		}
	}()

	return ch
}

func (tr *TerraformRunner) streamJsonEvents(ctx context.Context, args []string) <-chan StreamEvent {
	ch := make(chan StreamEvent)

	go func() {
		defer close(ch)

		cmd := tr.cmdFactory(ctx, tr.binary, args...)
		cmd.Dir = tr.workdir
		cmd.Cancel = func() error {
			return cmd.Process.Signal(os.Interrupt)
		}

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to pipe stdout: %w", err)}
			return
		}

		if err := cmd.Start(); err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("failed to terraform plan: %w", err)}
			return
		}

		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, MB), MB)

		for scanner.Scan() {
			event := ParseLine(scanner.Bytes())
			if event != nil {
				ch <- *event
			}
		}
		if err := scanner.Err(); err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("scanner error: %w", err)}
		}

		if err := cmd.Wait(); err != nil {
			ch <- StreamEvent{Error: fmt.Errorf("terraform plan exited with error: %w", err)}
		}
	}()

	return ch
}

func (tr *TerraformRunner) Taint(ctx context.Context, targets []string) <-chan StreamEvent {
	return tr.streamPerResource(ctx, "taint", targets)
}

func (tr *TerraformRunner) Untaint(ctx context.Context, targets []string) <-chan StreamEvent {
	return tr.streamPerResource(ctx, "untaint", targets)
}

func (tr *TerraformRunner) streamPerResource(ctx context.Context, command string, targets []string) <-chan StreamEvent {
	ch := make(chan StreamEvent)

	go func() {
		defer close(ch)

		for _, t := range targets {
			if ctx.Err() != nil {
				return
			}
			ch <- StreamEvent{
				Type:     MsgTypeApplyStart,
				Resource: &Resource{Address: t},
			}

			cmdArgs := tr.stackPrefix([]string{command, t})
			cmd := tr.cmdFactory(ctx, tr.binary, cmdArgs...)
			cmd.Dir = tr.workdir
			if tr.stackMode {
				tr.configureStackEnv(cmd)
			}
			output, err := cmd.CombinedOutput()

			if len(output) > 0 {
				ch <- StreamEvent{Message: strings.TrimSpace(string(output))}
			}

			eventType := MsgTypeApplyComplete
			if err != nil {
				eventType = MsgTypeApplyErrored
			}
			ch <- StreamEvent{
				Type:     eventType,
				Resource: &Resource{Address: t},
			}
		}
	}()

	return ch
}
