# Claude

T3 Code uses Claude Code's login and configuration. Start with the default provider
for one account; [provider setup](./install.md#providers) covers installation and
shared provider settings.

## Separate accounts or configurations

Use a separate Claude config directory for each account. This also works for named
presets that need different Claude settings or a router connection.

Keep your existing account in the default directory. On the environment's machine,
create the second login:

```bash
mkdir -p ~/.claude_personal
CLAUDE_CONFIG_DIR=~/.claude_personal claude auth login
```

Add another Claude instance in **Settings > Providers**:

| Instance        | Binary path | CLAUDE_CONFIG_DIR path |
| --------------- | ----------- | ---------------------- |
| Claude Work     | `claude`    | Leave empty            |
| Claude Personal | `claude`    | `~/.claude_personal`   |

An empty config-directory setting uses Claude Code's normal configuration. The
custom setting changes `CLAUDE_CONFIG_DIR`, leaving `HOME` and the system keychain
location intact. Use the same variable for the login command. Setting `HOME`
instead can put credentials where this provider will not find them.

Check the account reported in provider settings after signing in. By default,
existing threads can switch only between Claude instances with the same config
directory. Separate account directories stay isolated, including their local
conversation state. Memory sharing is configured separately below.

For presets that differ only in API keys or endpoints, use the instance's
**Environment variables**. Variable assignments do not belong in **Launch arguments**.

Claude Code's verbose mode can stay enabled when you use Claude for text generation, including
thread titles, branch names, commit messages, and pull request descriptions. On a remote connection,
T3 Code uses the Claude configuration on the connected server.

## Share memories, skills, and global instructions

Set **Shared Claude directory** to the same path on each Claude instance, for
example `~/.claude-shared`. Keep a different **Claude config directory** for each
account. The shared directory holds:

- Project memories, separated by repository and shared across its worktrees.
- User-scoped subagent memories.
- Personal skills in `skills/<name>/SKILL.md`, available in the composer and Claude.
- Global instructions in `CLAUDE.md` and Markdown files in `rules/`.

Account logins, settings, and conversation histories stay in their existing
directories. Repository instructions and project skills keep their normal scope.
Use a separate shared directory for Claude and Codex: their instructions, tools,
and skill behavior can differ.

The directory belongs to the connected environment's machine, including when
you use T3 Code remotely or from mobile. Provider terminals use the same shared
directory. Restart existing Claude sessions and provider terminals after changing
it. Clearing the field removes only the links T3 Code created, returning skills
and instructions to account-local locations. Shared files remain intact; they
are not copied back. Pre-existing links you created are left as they are. Memories
return to Claude's default, or an explicit `CLAUDE_CODE_REMOTE_MEMORY_DIR` from the
instance's environment.

To reuse one account's existing files, select its Claude config directory as the
shared directory, such as `~/.claude`. Other account directories must be outside
that directory. If another account already has a local `skills`, `CLAUDE.md`, or
`rules` entry, setup stops with a conflict. Stop its Claude sessions, back up and
reconcile the files into the shared directory, then move the conflicting local
entry to a backup location and retry. Existing files are never merged or
overwritten automatically. An explicit Claude `autoMemoryDirectory`
setting still overrides the default project-memory location.

This integration uses Claude Code's native memory-root support, verified with
Claude Code 2.1.274. It is independent of cross-account thread continuation.

## Cross-account thread continuation

Provider Settings includes **Cross-account thread continuation**, an opt-in alpha
feature for continuing a Claude thread through another configured Claude account.
It is off by default. When enabled, the thread's existing conversation context may
be sent through the account you switch to. A running turn always stays on the
account that started it; a switch applies to a later turn.

For an existing local Claude thread, T3 Code imports the source account's transcript
before the first cross-account switch. This is a local filesystem operation and does
not send a turn or consume Claude usage. After selecting another Claude provider,
use **Sync history now** in the composer banner to verify the import immediately;
otherwise it runs automatically when the next turn starts.

The imported transcript is copied into T3 Code's local shared session store and into
the target Claude config directory when that account resumes the thread. Automatic
cleanup is not implemented yet, so these local copies remain until the corresponding
T3 Code and Claude app data is removed.

Because this feature crosses account boundaries and uses an experimental continuation
path, only enable it when both configured accounts are allowed to receive the thread's
conversation context.

## Compact long conversations

Set **Auto-compact after** in the Claude provider settings to an integer between
`100000` and `1000000`. For example, `300000` asks Claude to summarize at about
300,000 tokens. This changes when compaction happens, not the model's context
window. Leave it empty for Claude Code's default.

You can also send `/compact` in an existing conversation. Web and desktop offer
**Compact context** from the context meter and may suggest it when you return to
a large older thread. See [commands and skills](./composer.md#commands-and-skills)
for using composer commands.

## Usage limits

If your Claude subscription runs out of usage mid-turn, the thread shows which
limit was reached and the remaining wait when Claude provides a reset time.
Claude Code holds the turn until that window reopens, so it can keep showing as
working. Wait for the reset, or stop the turn and continue later. The warning's
timestamp shows when the displayed wait started.

## Skills

Claude skills come from the config directory's `skills` folder (linked to the
shared directory when configured) and the project's
`.claude/skills` folder. If both define the same name, the config-directory copy
wins. Skills disabled in Claude's settings do not appear in the composer.

Use `$` in the composer to select a skill. Skills marked `disable-model-invocation`
can still be started by you. Invoke those one per message: Claude directly runs
only the last named skill and may try to start earlier ones through its Skill
tool, which refuses skills reserved for manual invocation.

## OpenRouter

Create a Claude instance with its own config directory, such as
`~/.claude_openrouter`, and keep **Binary path** set to `claude`. In that instance's
**Environment variables**, use:

| Variable               | Value                                     |
| ---------------------- | ----------------------------------------- |
| `ANTHROPIC_BASE_URL`   | `https://openrouter.ai/api`               |
| `ANTHROPIC_AUTH_TOKEN` | Your OpenRouter API key, marked Sensitive |
| `ANTHROPIC_API_KEY`    | An explicitly empty value                 |

If that Claude config directory has a cached Anthropic login, run `/logout` in a
Claude Code session using that directory before starting the router setup. Cached
login credentials can conflict with the router token.

Select the model you want in T3 Code. For an OpenRouter model outside the built-in
list, open that Claude instance in **Settings > Providers** and add its full model
ID with **Add custom model**. Then select it in the chat model picker.
`ANTHROPIC_DEFAULT_*_MODEL` variables map Claude Code aliases such as `sonnet`; they
do not replace the explicit model ID selected in T3 Code. Custom models may have
fewer effort, thinking, or context controls than built-in models.

Verify the model used in OpenRouter's activity dashboard. For current compatibility
requirements, use the
[OpenRouter Claude Code guide](https://openrouter.ai/docs/cookbook/coding-agents/claude-code-integration).

## Other routers

A local router uses an ordinary Claude provider instance. Give it a separate
config directory and put the router's endpoint and credential variables in that
instance's **Environment variables**. The router must run where the environment
can reach it. Follow the [Claude Code Router instructions](https://github.com/musistudio/claude-code-router)
for its installation and routing configuration.
