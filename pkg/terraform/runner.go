package terraform

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

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
		for _, t := range targets {
			args = append(args, fmt.Sprintf("-target=%s", t))
		}

		cmd := tr.cmdFactory(ctx, tr.binary, args...)
		cmd.Dir = tr.workdir
		cmd.Cancel = func() error {
			return cmd.Process.Signal(os.Interrupt)
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

		var lastStderr string
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				lastStderr = line
				ch <- StreamEvent{Message: line}
			}
		}

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
			ch <- StreamEvent{Error: fmt.Errorf("terragrunt stack run plan failed: %w\n%s", cmdErr, lastStderr)}
			return
		}

		for _, e := range events {
			ch <- e
		}
	}()

	return ch
}

func (tr *TerraformRunner) Apply(ctx context.Context, targets []string) <-chan StreamEvent {
	args := []string{"apply", "-auto-approve", "-json"}
	for _, t := range targets {
		args = append(args, fmt.Sprintf("-target=%s", t))
	}
	return tr.streamJsonEvents(ctx, tr.stackPrefix(args))
}

func (tr *TerraformRunner) Destroy(ctx context.Context, targets []string) <-chan StreamEvent {
	args := []string{"destroy", "-auto-approve", "-json"}
	for _, t := range targets {
		args = append(args, fmt.Sprintf("-target=%s", t))
	}
	return tr.streamJsonEvents(ctx, tr.stackPrefix(args))
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
