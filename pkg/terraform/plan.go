package terraform

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type PlanFile struct {
	FormatVersion   string           `json:"format_version"`
	ResourceChanges []ResourceChange `json:"resource_changes"`
}

type ResourceChange struct {
	Address       string       `json:"address"`
	ModuleAddress string       `json:"module_address"`
	Mode          string       `json:"mode"`
	Type          string       `json:"type"`
	Name          string       `json:"name"`
	Index         any          `json:"index"`
	ProviderName  string       `json:"provider_name"`
	Change        ChangeDetail `json:"change"`
}

type ChangeDetail struct {
	Actions []string        `json:"actions"`
	Before  json.RawMessage `json:"before"`
	After   json.RawMessage `json:"after"`
}

func planActionsToAction(actions []string) Action {
	if len(actions) == 1 {
		return normalizeAction(actions[0])
	}
	if len(actions) == 2 {
		return ActionReplace
	}
	return ActionNoop
}

func (rc *ResourceChange) toResource() Resource {
	resourceAddr := rc.Type + "." + rc.Name
	if rc.Mode == "data" {
		resourceAddr = "data." + resourceAddr
	}

	switch v := rc.Index.(type) {
	case string:
		resourceAddr += fmt.Sprintf("[%q]", v)
	case float64:
		resourceAddr += fmt.Sprintf("[%d]", int(v))
	}

	addr := resourceAddr
	if rc.ModuleAddress != "" {
		addr = rc.ModuleAddress + "." + addr
	}

	impliedProvider := rc.Type
	if under := strings.Index(impliedProvider, "_"); under != -1 {
		impliedProvider = impliedProvider[:under]
	}

	attrs := rc.Change.After
	if len(attrs) == 0 || string(attrs) == "null" {
		attrs = rc.Change.Before
	}

	return Resource{
		Address:         addr,
		Module:          rc.ModuleAddress,
		ResourceAddr:    resourceAddr,
		ResourceType:    rc.Type,
		ResourceName:    rc.Name,
		ResourceKey:     rc.Index,
		ImpliedProvider: impliedProvider,
		Action:          planActionsToAction(rc.Change.Actions),
		Attributes:      attrs,
	}
}

func parsePlanDir(dir string) ([]StreamEvent, error) {
	var events []StreamEvent

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read plan file %s: %w", path, err)
		}

		var pf PlanFile
		if err := json.Unmarshal(data, &pf); err != nil {
			return fmt.Errorf("failed to parse plan file %s: %w", path, err)
		}

		for i := range pf.ResourceChanges {
			r := pf.ResourceChanges[i].toResource()
			events = append(events, StreamEvent{
				Resource: &r,
				Type:     MsgTypePlannedChange,
			})
		}

		return nil
	})

	return events, err
}
