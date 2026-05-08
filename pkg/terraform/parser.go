package terraform

import (
	"encoding/json"
	"strings"

	"charm.land/log/v2"
)

// ParseLine parses single line of JSON into a StreamEvent
func ParseLine(line []byte) *StreamEvent {
	if len(line) == 0 {
		return nil
	}

	var msg Message
	if err := json.Unmarshal(line, &msg); err != nil {
		log.Debug("failed to parse plan JSON", "error", err, "line", string(line))
		return nil
	}

	msgType := MessageType(msg.Type)
	var event *StreamEvent
	switch msgType {
	case MsgTypeRefreshStart, MsgTypeRefreshComplete, MsgTypeApplyStart, MsgTypeApplyProgress, MsgTypeApplyComplete, MsgTypeApplyErrored:
		if msg.Hook != nil {
			event = &StreamEvent{
				Resource: extractResourceInfo(&msg.Hook.Resource, normalizeAction(msg.Hook.Action), ""),
				Type:     msgType,
			}
		}
	case MsgTypeResourceDrift:
		if msg.Change != nil {
			event = &StreamEvent{
				Resource: extractResourceInfo(&msg.Change.Resource, normalizeAction(msg.Change.Action), "drift"),
				Type:     msgType,
			}
		}
	case MsgTypePlannedChange:
		if msg.Change != nil {
			event = &StreamEvent{
				Resource: extractResourceInfo(&msg.Change.Resource, normalizeAction(msg.Change.Action), msg.Change.Reason),
				Type:     msgType,
			}
		}
	case MsgTypeDiagnostic:
		if msg.Diagnostic != nil {
			event = &StreamEvent{
				Diagnostic: msg.Diagnostic,
				Type:       msgType,
			}
		}
	case MsgTypeChangeSummary:
		if msg.Changes != nil {
			event = &StreamEvent{
				Summary: msg.Changes,
				Type:    msgType,
			}
		}
	case MsgTypeOutputs:
		if msg.Outputs != nil {
			event = &StreamEvent{
				Outputs: msg.Outputs,
				Type:    msgType,
			}
		}
	default:
		return nil
	}

	if event == nil {
		return nil
	}
	event.Message = msg.Message

	return event
}

func extractResourceInfo(info *ResourceInfo, action Action, reason string) *Resource {
	return &Resource{
		Address:         info.Addr,
		Module:          info.Module,
		ResourceAddr:    info.Resource,
		ResourceType:    info.ResourceType,
		ResourceName:    info.ResourceName,
		ResourceKey:     info.ResourceKey,
		ImpliedProvider: info.ImpliedProvider,
		Action:          action,
		Reason:          reason,
	}
}

// ParseTextLine extracts resource status from terraform's human-readable output.
// Patterns: "aws_instance.web: Creating...", "aws_instance.web: Creation complete after 5s [id=i-xxx]"
func ParseTextLine(line string) *StreamEvent {
	colonIdx := strings.Index(line, ": ")
	if colonIdx < 0 {
		return nil
	}

	addr := strings.TrimSpace(line[:colonIdx])
	rest := line[colonIdx+2:]

	if !looksLikeResourceAddr(addr) {
		return nil
	}

	var msgType MessageType
	switch {
	case strings.HasPrefix(rest, "Refreshing state"):
		msgType = MsgTypeRefreshStart
	case strings.HasPrefix(rest, "Creating"),
		strings.HasPrefix(rest, "Modifying"),
		strings.HasPrefix(rest, "Destroying"),
		strings.HasPrefix(rest, "Reading"):
		if strings.Contains(rest, "complete") {
			msgType = MsgTypeApplyComplete
		} else if strings.HasPrefix(rest, "Still") {
			msgType = MsgTypeApplyProgress
		} else {
			msgType = MsgTypeApplyStart
		}
	case strings.HasPrefix(rest, "Still creating"),
		strings.HasPrefix(rest, "Still modifying"),
		strings.HasPrefix(rest, "Still destroying"),
		strings.HasPrefix(rest, "Still reading"):
		msgType = MsgTypeApplyProgress
	case strings.HasPrefix(rest, "Creation complete"),
		strings.HasPrefix(rest, "Modifications complete"),
		strings.HasPrefix(rest, "Destruction complete"),
		strings.HasPrefix(rest, "Read complete"):
		msgType = MsgTypeApplyComplete
	default:
		return nil
	}

	return &StreamEvent{
		Type:     msgType,
		Resource: &Resource{Address: addr},
		Message:  line,
	}
}

func looksLikeResourceAddr(s string) bool {
	if !strings.Contains(s, ".") {
		return false
	}
	if strings.ContainsAny(s, " \t/\\") {
		return false
	}
	parts := strings.SplitN(s, ".", 2)
	first := parts[0]
	return first == "data" || first == "module" || strings.Contains(first, "_")
}
