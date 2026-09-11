/* eslint-disable react/no-array-index-key -- These controlled form rows have editable IDs and duplicate argument values; keying by text would remount inputs on every keystroke. */
import {
  AGENT_CONTROL_ROLES,
  type AgentControlProjectPolicy,
  type AgentControlRoleRoute,
  type AgentControlVerificationCheck,
  type ModelSelection,
  type RepositoryIdentity,
  type ServerConfig,
} from "@t3tools/contracts";
import {
  agentControlSetupErrorMessage,
  agentControlPreflightErrorMessage,
  type createAgentControlSetupController,
} from "@t3tools/client-runtime/state/agent-control-setup";
import { useState, useSyncExternalStore } from "react";
import { Pressable, Switch, View } from "react-native";

import { AppText as Text, AppTextInput as TextInput } from "../../components/AppText";

type Controller = ReturnType<typeof createAgentControlSetupController>;

function Action({
  children,
  onPress,
  disabled = false,
}: {
  children: string;
  onPress: () => void;
  disabled?: boolean;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityState={{ disabled }}
      disabled={disabled}
      onPress={onPress}
      className={`rounded-xl border border-border-subtle px-3 py-2 ${disabled ? "opacity-40" : "bg-card"}`}
    >
      <Text className="text-sm">{children}</Text>
    </Pressable>
  );
}

function Field({
  label,
  value,
  onChange,
  disabled,
  numeric = false,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
  numeric?: boolean;
}) {
  return (
    <View className="gap-1">
      <Text className="text-xs text-foreground-muted">{label}</Text>
      <TextInput
        accessibilityLabel={label}
        value={value}
        onChangeText={onChange}
        editable={!disabled}
        autoCapitalize="none"
        autoCorrect={false}
        keyboardType={numeric ? "number-pad" : "default"}
        className="rounded-lg border border-border-subtle bg-card p-3 text-foreground"
      />
    </View>
  );
}

function Toggle({
  label,
  value,
  onChange,
  disabled,
}: {
  label: string;
  value: boolean;
  onChange: (value: boolean) => void;
  disabled: boolean;
}) {
  return (
    <View className="flex-row items-center justify-between gap-3">
      <Text className="flex-1 text-sm">{label}</Text>
      <Switch
        accessibilityLabel={label}
        value={value}
        onValueChange={onChange}
        disabled={disabled}
      />
    </View>
  );
}

function StringList({
  label,
  values,
  onChange,
  disabled,
}: {
  label: string;
  values: ReadonlyArray<string>;
  onChange: (values: string[]) => void;
  disabled: boolean;
}) {
  return (
    <View className="gap-2">
      <Text className="text-sm">{label}</Text>
      {values.map((value, index) => (
        <View key={index} className="gap-1">
          <Field
            label={`${label} ${index + 1}`}
            value={value}
            disabled={disabled}
            onChange={(next) => onChange(values.map((item, i) => (i === index ? next : item)))}
          />
          <Action
            disabled={disabled}
            onPress={() => onChange(values.filter((_, i) => i !== index))}
          >
            Remove item
          </Action>
        </View>
      ))}
      <Action disabled={disabled} onPress={() => onChange([...values, ""])}>
        Add item
      </Action>
    </View>
  );
}

function Candidates({
  values,
  onChange,
  providers,
  disabled,
}: {
  values: ReadonlyArray<ModelSelection>;
  onChange: (values: ModelSelection[]) => void;
  providers: ServerConfig["providers"];
  disabled: boolean;
}) {
  const [choosing, setChoosing] = useState(false);
  return (
    <View className="gap-2">
      {values.map((value, index) => (
        <View key={index} className="gap-2 rounded-lg border border-border-subtle p-2">
          <Text className="text-sm">
            {index === 0 ? "First choice" : `Fallback ${index}`} · {value.instanceId} ·{" "}
            {value.model}
          </Text>
          <View className="flex-row flex-wrap gap-2">
            <Action
              disabled={disabled || index === 0}
              onPress={() => {
                const next = [...values];
                [next[index - 1], next[index]] = [next[index]!, next[index - 1]!];
                onChange(next);
              }}
            >
              Move earlier
            </Action>
            <Action
              disabled={disabled}
              onPress={() => onChange(values.filter((_, i) => i !== index))}
            >
              Remove candidate
            </Action>
          </View>
        </View>
      ))}
      <Action disabled={disabled} onPress={() => setChoosing(!choosing)}>
        {choosing ? "Close model choices" : "Add provider / model"}
      </Action>
      {choosing ? (
        <View className="gap-2">
          {providers.length === 0 ? (
            <Text className="text-sm text-foreground-muted">
              No provider instances reported by this environment.
            </Text>
          ) : null}
          {providers.map((provider) => (
            <View key={provider.instanceId} className="gap-2">
              <Text className="text-sm font-t3-bold">
                {provider.displayName ?? provider.instanceId} · {provider.driver}
                {!provider.enabled ? " · Disabled" : ""}
              </Text>
              {provider.models.map((model) => (
                <Action
                  key={model.slug}
                  disabled={disabled || !provider.enabled}
                  onPress={() => {
                    onChange([...values, { instanceId: provider.instanceId, model: model.slug }]);
                    setChoosing(false);
                  }}
                >
                  {model.slug}
                </Action>
              ))}
            </View>
          ))}
        </View>
      ) : null}
    </View>
  );
}

function CheckEditor({
  check,
  onChange,
  onRemove,
  disabled,
}: {
  check: AgentControlVerificationCheck;
  onChange: (check: AgentControlVerificationCheck) => void;
  onRemove: () => void;
  disabled: boolean;
}) {
  return (
    <View className="gap-3 rounded-xl border border-border-subtle p-3">
      <Field
        label="Check ID"
        value={check.id}
        onChange={(id) => onChange({ ...check, id })}
        disabled={disabled}
      />
      <Field
        label="Executable program"
        value={check.command}
        onChange={(command) => onChange({ ...check, command })}
        disabled={disabled}
      />
      <StringList
        label="Arguments (one field per argument)"
        values={check.args}
        onChange={(args) => onChange({ ...check, args })}
        disabled={disabled}
      />
      <Field
        label="Working directory relative to task worktree"
        value={check.cwd}
        onChange={(cwd) => onChange({ ...check, cwd })}
        disabled={disabled}
      />
      <Field
        label="Timeout in milliseconds (1–300000)"
        value={String(check.timeoutMs)}
        onChange={(value) => onChange({ ...check, timeoutMs: Number(value) })}
        disabled={disabled}
        numeric
      />
      <Toggle
        label="Required check"
        value={check.required}
        onChange={(required) => onChange({ ...check, required })}
        disabled={disabled}
      />
      <Toggle
        label="Allow temporary files"
        value={check.allowTemporaryFiles}
        onChange={(allowTemporaryFiles) => onChange({ ...check, allowTemporaryFiles })}
        disabled={disabled}
      />
      <Text className="text-xs text-foreground-muted">Result format</Text>
      <View className="flex-row flex-wrap gap-2">
        {(["exit-code", "node-test", "vitest-json"] as const).map((format) => (
          <Action
            key={format}
            disabled={disabled}
            onPress={() => onChange({ ...check, resultFormat: format })}
          >{`${check.resultFormat === format ? "✓ " : ""}${format}`}</Action>
        ))}
      </View>
      <Action disabled={disabled} onPress={onRemove}>
        Remove check
      </Action>
    </View>
  );
}

export function AutonomousProjectSetupForm({
  controller,
  providers,
  repositoryIdentity,
  writeBlocker,
  importBlocker,
  connected,
  onSaved,
}: {
  controller: Controller;
  providers: ServerConfig["providers"];
  repositoryIdentity: RepositoryIdentity | null;
  writeBlocker: string | null;
  importBlocker: string | null;
  connected: boolean;
  onSaved: () => void;
}) {
  const state = useSyncExternalStore(
    controller.subscribe,
    controller.getSnapshot,
    controller.getSnapshot,
  );
  const [expanded, setExpanded] = useState(false);
  const [roleOpen, setRoleOpen] = useState<string | null>(null);
  const disabled = state.pending || !state.loaded || writeBlocker !== null;
  const policy = state.policyDraft;
  const github = state.githubDraft;
  const changePolicy = (patch: Partial<AgentControlProjectPolicy>) => {
    if (policy) controller.setPolicy({ ...policy, ...patch });
  };
  const removeOverride = (key: keyof AgentControlProjectPolicy) => {
    if (policy) {
      const next = { ...policy };
      delete next[key];
      controller.setPolicy(next);
    }
  };
  const save = async (action: () => Promise<unknown>) => {
    await action();
    onSaved();
  };
  return (
    <View className="gap-3 rounded-2xl border border-border-subtle p-4">
      <Text className="text-base font-t3-bold">Project setup</Text>
      <Text className="text-sm text-foreground-muted">
        These settings belong to the project and environment above. Saving does not start Run once
        or turn on automation. An active run can read changed settings for later stages; a stage
        already admitted keeps its captured configuration.
      </Text>
      <Text accessibilityLiveRegion="polite" className="text-sm">
        {state.pending
          ? "Working…"
          : state.policyDirty || state.githubDirty
            ? "Unsaved changes"
            : "No unsaved changes"}
      </Text>
      <Text className="text-sm text-foreground-muted">
        {state.githubState?.config
          ? `GitHub: ${state.githubState.config.repository.nameWithOwner}`
          : "GitHub intake: not configured"}{" "}
        · {state.policyState?.projectPolicy ? "Project policy saved" : "Using inherited routing"}
        {` · Required checks saved: ${state.policyState?.projectPolicy?.policy.verificationChecks?.filter((check) => check.required).length ?? 0}`}
      </Text>
      {state.error ? (
        <Text selectable className="text-sm text-destructive">
          {state.error}
        </Text>
      ) : null}
      {state.notice ? (
        <Text accessibilityLiveRegion="polite" className="text-sm">
          {state.notice}
        </Text>
      ) : null}
      {writeBlocker ? <Text className="text-sm text-foreground-muted">{writeBlocker}</Text> : null}
      <Action onPress={() => setExpanded(!expanded)}>
        {expanded ? "Hide setup" : "Configure project"}
      </Action>
      {expanded ? (
        <>
          <View className="flex-row flex-wrap gap-2">
            <Action
              disabled={!connected || state.pending}
              onPress={() => void controller.discard()}
            >
              {state.policyDirty || state.githubDirty
                ? "Reload saved settings (discard edits)"
                : "Reload saved settings"}
            </Action>
            <Action
              disabled={!connected || state.pending || (!state.policyDirty && !state.githubDirty)}
              onPress={() => controller.discard()}
            >
              Discard unsaved changes
            </Action>
          </View>
          <Text className="text-base font-t3-bold">GitHub intake</Text>
          <Text selectable className="text-sm">
            Checkout repository:{" "}
            {repositoryIdentity?.displayName ??
              repositoryIdentity?.canonicalKey ??
              "Not reported by this environment"}
            {repositoryIdentity ? ` · Remote: ${repositoryIdentity.locator.remoteName}` : ""}
          </Text>
          <Text className="text-sm text-foreground-muted">
            This is the environment's last reported checkout identity. The server selects upstream,
            then origin, then the first remote alphabetically and verifies the repository and access
            again when saving. The saved intake binding below is separate from this checkout
            identity. GitHub authentication must already be configured on that environment.
          </Text>
          {state.githubState?.config ? (
            <Text selectable className="text-xs text-foreground-muted">
              Saved repository: {state.githubState.config.repository.nameWithOwner} ·{" "}
              {state.githubState.config.repository.repositoryNodeId}
            </Text>
          ) : null}
          {github ? (
            <>
              <Field
                label="Ready label"
                value={github.readyLabel}
                disabled={disabled}
                onChange={(readyLabel) => controller.setGithub({ ...github, readyLabel })}
              />
              <Field
                label="Pause label"
                value={github.pausedLabel}
                disabled={disabled}
                onChange={(pausedLabel) => controller.setGithub({ ...github, pausedLabel })}
              />
              <StringList
                label="Trusted GitHub accounts"
                values={github.trustedLogins}
                disabled={disabled}
                onChange={(trustedLogins) => controller.setGithub({ ...github, trustedLogins })}
              />
              <Field
                label="Poll interval in seconds (15–3600)"
                value={String(github.pollIntervalSeconds)}
                disabled={disabled}
                numeric
                onChange={(value) =>
                  controller.setGithub({ ...github, pollIntervalSeconds: Number(value) })
                }
              />
              <Action
                disabled={disabled || (!state.githubDirty && state.githubState?.config !== null)}
                onPress={() => void save(controller.saveGithub)}
              >
                Save GitHub intake
              </Action>
            </>
          ) : null}
          <Action
            disabled={disabled || !state.githubState?.config}
            onPress={() => void save(controller.clearGithub)}
          >
            Remove GitHub intake configuration
          </Action>
          {importBlocker ? (
            <Text className="text-xs text-foreground-muted">{importBlocker}</Text>
          ) : null}
          <Action
            disabled={
              state.pending ||
              !state.loaded ||
              importBlocker !== null ||
              !state.githubState?.config ||
              state.githubDirty
            }
            onPress={() => void save(controller.importIssues)}
          >
            Import / refresh GitHub issues
          </Action>
          <Text className="text-xs text-foreground-muted">
            Last import: {state.githubState?.pollStatus.status ?? "Unknown"}
            {state.githubState?.pollStatus.status === "success"
              ? ` · ${state.githubState.pollStatus.issueCount} issues · ${state.githubState.pollStatus.completedAt}`
              : ""}
            {state.githubState?.pollStatus.errorCode
              ? ` · ${state.githubState.pollStatus.errorCode}`
              : ""}
          </Text>
          {state.githubState?.pollStatus.errorCode ? (
            <Text className="text-sm text-destructive">
              {agentControlSetupErrorMessage({ code: state.githubState.pollStatus.errorCode })}
            </Text>
          ) : null}
          <Text className="text-base font-t3-bold">Provider and model routing</Text>
          {policy ? (
            <>
              <Text className="text-sm text-foreground-muted">
                Candidates run in order when supported. Missing project values inherit environment
                defaults. All six roles keep their existing access rules. Full access:{" "}
                {policy.fullAccess === undefined
                  ? "default restricted"
                  : policy.fullAccess
                    ? "enabled for Implementation / Repair"
                    : "disabled"}
                .
              </Text>
              <Toggle
                label="Full access for Implementation and Repair"
                value={policy.fullAccess === true}
                disabled={disabled}
                onChange={(fullAccess) => changePolicy({ fullAccess })}
              />
              <Text className="text-xs text-foreground-muted">
                Explicitly permits full access for these two roles on this environment. Planning,
                orchestration, review and verification remain restricted. Saving does not start
                work.
              </Text>
              {policy.fullAccess !== undefined ? (
                <Action disabled={disabled} onPress={() => removeOverride("fullAccess")}>
                  Reset access to default (restricted)
                </Action>
              ) : null}
              <Text className="text-sm">
                Provider admission ·{" "}
                {policy.providerAllowlist === undefined ? "Inherited" : "Project override"}
              </Text>
              {policy.providerAllowlist === undefined ? (
                <Action
                  disabled={disabled}
                  onPress={() =>
                    changePolicy({
                      providerAllowlist:
                        state.policyState?.appPolicy?.providerAllowlist ??
                        providers.map((provider) => provider.instanceId),
                    })
                  }
                >
                  Customize allowed providers
                </Action>
              ) : (
                <>
                  {providers.map((provider) => (
                    <Toggle
                      key={provider.instanceId}
                      label={provider.displayName ?? provider.instanceId}
                      value={policy.providerAllowlist!.includes(provider.instanceId)}
                      disabled={disabled}
                      onChange={(enabled) =>
                        changePolicy({
                          providerAllowlist: enabled
                            ? [...policy.providerAllowlist!, provider.instanceId]
                            : policy.providerAllowlist!.filter((id) => id !== provider.instanceId),
                        })
                      }
                    />
                  ))}
                  <Action disabled={disabled} onPress={() => removeOverride("providerAllowlist")}>
                    Inherit allowed providers
                  </Action>
                </>
              )}
              <Text className="text-sm">
                Default fallbacks ·{" "}
                {policy.defaultFallbacks === undefined ? "Inherited" : "Project override"}
              </Text>
              {policy.defaultFallbacks === undefined ? (
                <>
                  <Text className="text-xs text-foreground-muted">
                    {state.policyState?.appPolicy?.defaultFallbacks
                      ?.map((candidate) => `${candidate.instanceId} / ${candidate.model}`)
                      .join(", ") ??
                      "Built-in defaults; resolved candidates appear in preflight below."}
                  </Text>
                  <Action
                    disabled={disabled}
                    onPress={() =>
                      changePolicy({
                        defaultFallbacks: state.policyState?.appPolicy?.defaultFallbacks ?? [],
                      })
                    }
                  >
                    Customize default fallbacks
                  </Action>
                </>
              ) : (
                <>
                  <Candidates
                    values={policy.defaultFallbacks}
                    onChange={(defaultFallbacks) => changePolicy({ defaultFallbacks })}
                    providers={providers}
                    disabled={disabled}
                  />
                  <Action disabled={disabled} onPress={() => removeOverride("defaultFallbacks")}>
                    Inherit default fallbacks
                  </Action>
                </>
              )}
              {AGENT_CONTROL_ROLES.map((role) => {
                const route = policy.roleRoutes?.[role];
                const inherited = state.policyState?.appPolicy?.roleRoutes?.[role];
                const update = (next: AgentControlRoleRoute) =>
                  changePolicy({ roleRoutes: { ...policy.roleRoutes, [role]: next } });
                return (
                  <View key={role} className="gap-2 border-t border-border-subtle pt-3">
                    <Action
                      onPress={() => setRoleOpen(roleOpen === role ? null : role)}
                    >{`${role} · ${route ? "Project override" : inherited ? "Inherited role route" : "Default fallbacks"}`}</Action>
                    {roleOpen === role ? (
                      route ? (
                        <>
                          <Candidates
                            values={route.candidates}
                            onChange={(candidates) => update({ ...route, candidates })}
                            providers={providers}
                            disabled={disabled}
                          />
                          <Toggle
                            label="Strict role route (do not append default fallbacks)"
                            value={route.strict}
                            onChange={(strict) => update({ ...route, strict })}
                            disabled={disabled}
                          />
                          <Text className="text-xs text-foreground-muted">Driver restriction</Text>
                          <View className="flex-row flex-wrap gap-2">
                            {[...new Set(providers.map((provider) => provider.driver))].map(
                              (driverKind) => (
                                <Action
                                  key={driverKind}
                                  disabled={disabled}
                                  onPress={() => update({ ...route, driverKind })}
                                >
                                  {`${route.driverKind === driverKind ? "✓ " : ""}${driverKind}`}
                                </Action>
                              ),
                            )}
                          </View>
                          {route.driverKind ? (
                            <>
                              <Text className="text-sm">
                                Driver restriction: {route.driverKind}
                              </Text>
                              <Action
                                disabled={disabled}
                                onPress={() => {
                                  const next = { ...route };
                                  delete next.driverKind;
                                  update(next);
                                }}
                              >
                                Remove driver restriction
                              </Action>
                            </>
                          ) : null}
                          <Action
                            disabled={disabled}
                            onPress={() => {
                              const roleRoutes = { ...policy.roleRoutes };
                              delete roleRoutes[role];
                              changePolicy({ roleRoutes });
                            }}
                          >
                            Inherit this role
                          </Action>
                        </>
                      ) : (
                        <>
                          <Text className="text-xs text-foreground-muted">
                            {inherited?.candidates
                              .map((candidate) => `${candidate.instanceId} / ${candidate.model}`)
                              .join(", ") ?? "Uses the ordered default fallbacks."}
                          </Text>
                          <Action
                            disabled={disabled}
                            onPress={() => update(inherited ?? { candidates: [], strict: false })}
                          >
                            Customize this role
                          </Action>
                        </>
                      )
                    ) : null}
                  </View>
                );
              })}
              <Text className="text-base font-t3-bold">Verification checks</Text>
              <Text className="text-sm text-foreground-muted">
                Checks execute on the selected environment inside the task worktree. Each argument
                is passed separately, without shell conversion. Working directories must stay within
                that worktree. Saving and preflight do not execute checks or prove verification
                passed.
              </Text>
              {(policy.verificationChecks ?? []).map((check, index) => (
                <CheckEditor
                  key={index}
                  check={check}
                  disabled={disabled}
                  onChange={(next) =>
                    changePolicy({
                      verificationChecks: policy.verificationChecks!.map((item, i) =>
                        i === index ? next : item,
                      ),
                    })
                  }
                  onRemove={() =>
                    changePolicy({
                      verificationChecks: policy.verificationChecks!.filter((_, i) => i !== index),
                    })
                  }
                />
              ))}
              <Action
                disabled={disabled || (policy.verificationChecks?.length ?? 0) >= 32}
                onPress={() =>
                  changePolicy({
                    verificationChecks: [
                      ...(policy.verificationChecks ?? []),
                      {
                        id: `check-${(policy.verificationChecks?.length ?? 0) + 1}`,
                        command: "",
                        args: [],
                        cwd: ".",
                        required: true,
                        timeoutMs: 60000,
                        allowTemporaryFiles: false,
                        resultFormat: "exit-code",
                      },
                    ],
                  })
                }
              >
                Add verification check
              </Action>
              <Action
                disabled={disabled || !state.policyDirty}
                onPress={() => void save(controller.savePolicy)}
              >
                Save routing and checks
              </Action>
              <Action
                disabled={!connected || !state.loaded || state.pending}
                onPress={() => void controller.preflight()}
              >
                Check provider readiness for draft
              </Action>
              {state.preflight ? (
                <Text
                  className={`text-sm ${state.preflight.ok ? "text-foreground-muted" : "text-destructive"}`}
                >
                  {state.preflight.ok
                    ? "Provider preflight passed. Verification checks have not run."
                    : `Provider preflight blocked: ${[
                        ...state.preflight.roles
                          .filter((role) => role.errorCode)
                          .map(
                            (role) =>
                              `${role.role}: ${agentControlPreflightErrorMessage(role.errorCode!)}`,
                          ),
                        ...state.preflight.roles.flatMap((role) =>
                          role.candidates
                            .filter((candidate) => candidate.errorCode)
                            .map(
                              (candidate) =>
                                `${role.role} · ${candidate.providerInstanceId} / ${candidate.model}: ${agentControlPreflightErrorMessage(candidate.errorCode!)}`,
                            ),
                        ),
                        ...(state.preflight.staticPreflight.ok
                          ? []
                          : state.preflight.staticPreflight.errors.map(
                              (error) =>
                                `${error.role}: ${agentControlPreflightErrorMessage(error.code)}`,
                            )),
                      ].join(
                        "; ",
                      )}. Check allowed providers, model availability, authentication and role fallbacks in this environment.`}
                </Text>
              ) : null}
            </>
          ) : (
            <Text className="text-sm text-foreground-muted">Loading project policy…</Text>
          )}
        </>
      ) : null}
    </View>
  );
}
