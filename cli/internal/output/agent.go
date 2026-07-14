package output

import "os"

// AgentEnvVars are environment variables set by common AI coding agents and
// assistant harnesses. Their presence is a strong signal that CLI output will
// be consumed by an LLM rather than read by a human — in which case a
// token-efficient format (e.g. TOON) is usually preferable.
//
// This is detection only: the library does not impose a format policy. The
// consuming CLI decides what to do with the signal (see RegisterFlagsWithDefault).
// It is exported so a consumer can extend or replace the list.
var AgentEnvVars = []string{
	"CLAUDECODE",                     // Claude Code
	"CLAUDE_CODE_ENTRYPOINT",         // Claude Code
	"CURSOR_AGENT",                   // Cursor agent mode
	"CURSOR_TRACE_ID",                // Cursor
	"CODEX_SANDBOX",                  // OpenAI Codex CLI
	"CODEX_SANDBOX_NETWORK_DISABLED", // OpenAI Codex CLI
	"AIDER_CHAT",                     // aider
	"OPENAI_AGENT",                   // generic
}

// IsAgentContext reports whether any known AI-agent environment marker is set,
// suggesting the CLI was invoked by an automated assistant rather than a human.
func IsAgentContext() bool {
	for _, k := range AgentEnvVars {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}
