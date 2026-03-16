<script lang="ts">
  import { onMount } from 'svelte';
  import { engineState, engineStore } from '$lib/stores/engineStore';
  import { toastStore } from '$lib/stores/toastStore';

  type StatusResponse = { active_runs: number; queued: number; completed_total: number };
  type RunRecord = {
    job_id: string;
    pipeline_id: string;
    pipeline_version: number;
    status: 'Queued' | 'Running' | 'Completed' | 'Failed' | 'Cancelled';
    submitted_at: string;
    duration_secs: number | null;
    error: string | null;
  };

  let status = $state<StatusResponse>({
    active_runs: 0,
    queued: 0,
    completed_total: 0
  });
  let runs = $state<RunRecord[]>([]);
  let expandedJobId = $state<string | null>(null);
  let jobDetails = $state<Record<string, { node_progress: Array<{ node_id: string; status: string; rows_processed: number | null }> }>>({});
  let polling = $state<number | null>(null);
  let connecting = $state(false);
  let loadingRuns = $state(false);
  let statusError = $state<string | null>(null);
  let runsError = $state<string | null>(null);
  let previousCompletedTotal = $state<number | null>(null);

  const activeRuns = $derived(runs.filter((run) => run.status === 'Running' || run.status === 'Queued'));

  async function connect() {
    connecting = true;
    const ok = await engineStore.connect();
    connecting = false;
    if (!ok) {
      toastStore.push({ kind: 'error', message: 'Failed to connect to engine' });
      return;
    }
    toastStore.push({ kind: 'success', message: 'Connected to engine' });
    await refreshStatus();
    await loadRuns();
    startPolling();
  }

  function disconnect() {
    stopPolling();
    engineStore.setDisconnected();
    status = { active_runs: 0, queued: 0, completed_total: 0 };
    toastStore.push({ kind: 'info', message: 'Disconnected from engine' });
  }

  async function refreshStatus() {
    statusError = null;
    try {
      const next = await engineStore.fetchStatus();
      status = next;
      if (previousCompletedTotal !== null && next.completed_total > previousCompletedTotal) {
        await loadRuns();
      }
      previousCompletedTotal = next.completed_total;
      if (next.active_runs > 0) {
        await refreshActiveDetails();
      }
    } catch (error) {
      statusError = `${error}`;
      engineStore.setDisconnected();
      stopPolling();
    }
  }

  async function loadRuns() {
    loadingRuns = true;
    runsError = null;
    try {
      const list = (await engineStore.fetchRuns()) as RunRecord[];
      runs = list;
    } catch (error) {
      runsError = `${error}`;
    } finally {
      loadingRuns = false;
    }
  }

  async function refreshActiveDetails() {
    for (const run of activeRuns) {
      try {
        const detail = await engineStore.fetchPipelineStatus(run.job_id);
        jobDetails = { ...jobDetails, [run.job_id]: detail };
      } catch {}
    }
  }

  function startPolling() {
    stopPolling();
    polling = window.setInterval(() => {
      void refreshStatus();
    }, 3000);
  }

  function stopPolling() {
    if (polling !== null) {
      clearInterval(polling);
      polling = null;
    }
  }

  async function toggleRow(jobId: string) {
    expandedJobId = expandedJobId === jobId ? null : jobId;
    if (!expandedJobId) return;
    if (jobDetails[jobId]) return;
    try {
      const detail = await engineStore.fetchPipelineStatus(jobId);
      jobDetails = { ...jobDetails, [jobId]: detail };
    } catch {}
  }

  function progressFor(jobId: string) {
    const nodes = jobDetails[jobId]?.node_progress ?? [];
    const total = nodes.length;
    const done = nodes.filter((node) => node.status === 'Done').length;
    const running = nodes.find((node) => node.status === 'Running');
    const percent = total === 0 ? 0 : Math.round((done / total) * 100);
    return {
      percent,
      currentNode: running?.node_id ?? (done === total && total > 0 ? 'Completed' : 'Waiting')
    };
  }

  async function cancelRun(jobId: string) {
    const ok = await engineStore.cancelPipeline(jobId);
    if (ok) {
      toastStore.push({ kind: 'info', message: `Cancelled run ${jobId}` });
      await refreshStatus();
      await loadRuns();
    } else {
      toastStore.push({ kind: 'error', message: `Failed to cancel run ${jobId}` });
    }
  }

  onMount(() => {
    if ($engineState.connected) {
      void refreshStatus();
      void loadRuns();
      startPolling();
    }
    return () => stopPolling();
  });
</script>

<div class="h-[calc(100vh-49px)] overflow-auto bg-warm-bg p-4">
  <div class="max-w-5xl mx-auto flex flex-col gap-4">
    <section class="bg-warm-panel border border-warm-border rounded p-4">
      <h2 class="text-warm-text font-bold mb-3">Engine Connection</h2>
      <div class="grid grid-cols-1 md:grid-cols-[1fr_1fr_auto_auto] gap-2">
        <input
          class="px-3 py-2 text-sm border border-warm-border rounded bg-white text-warm-text outline-none"
          placeholder="http://127.0.0.1:7070"
          value={$engineState.url}
          oninput={(e) => engineStore.setUrl((e.currentTarget as HTMLInputElement).value)}
        />
        <input
          type="password"
          class="px-3 py-2 text-sm border border-warm-border rounded bg-white text-warm-text outline-none"
          placeholder="API key"
          value={$engineState.apiKey}
          oninput={(e) => engineStore.setApiKey((e.currentTarget as HTMLInputElement).value)}
        />
        <button
          type="button"
          class="px-3 py-2 text-sm border border-warm-border rounded bg-white text-warm-text hover:bg-warm-light transition-colors"
          onclick={() => void connect()}
          disabled={connecting}
        >{connecting ? 'Connecting…' : 'Connect'}</button>
        <button
          type="button"
          class="px-3 py-2 text-sm border border-warm-border rounded bg-white text-warm-text hover:bg-warm-light transition-colors"
          onclick={disconnect}
          disabled={!$engineState.connected}
        >Disconnect</button>
      </div>
      <div class="mt-3 flex items-center gap-3 text-sm">
        <div class="flex items-center gap-2">
          <span class="h-2 w-2 rounded-full" class:bg-[#4A7C59]={$engineState.connected} class:bg-[#B85C4A]={!$engineState.connected}></span>
          <span class="text-warm-sub">{$engineState.connected ? 'Connected' : 'Disconnected'}</span>
        </div>
        {#if $engineState.connected && $engineState.uptime !== null}
          <div class="text-warm-sub">Uptime: {$engineState.uptime}s</div>
        {/if}
      </div>
    </section>

    <section class="bg-warm-panel border border-warm-border rounded p-4">
      <h2 class="text-warm-text font-bold mb-3">Active Runs</h2>
      {#if statusError}
        <div class="text-xs text-[#B85C4A] mb-2">{statusError}</div>
      {/if}
      <div class="text-xs text-warm-sub mb-3">
        Active: {status.active_runs} · Queued: {status.queued}
      </div>
      {#if activeRuns.length === 0}
        <div class="text-xs text-warm-muted">No active runs</div>
      {:else}
        <div class="flex flex-col gap-2">
          {#each activeRuns as run (run.job_id)}
            {@const progress = progressFor(run.job_id)}
            <div class="border border-warm-border rounded bg-white p-3">
              <div class="flex items-center justify-between text-sm">
                <div class="text-warm-text font-medium">{run.pipeline_id} v{run.pipeline_version}</div>
                <button
                  type="button"
                  class="px-2 py-1 text-xs border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors"
                  onclick={() => void cancelRun(run.job_id)}
                >Cancel</button>
              </div>
              <div class="mt-2 h-2 bg-warm-bg rounded overflow-hidden">
                <div class="h-full bg-[#4A7C59]" style={`width: ${progress.percent}%`}></div>
              </div>
              <div class="mt-1 text-xs text-warm-sub">{progress.percent}% · {progress.currentNode}</div>
            </div>
          {/each}
        </div>
      {/if}
    </section>

    <section class="bg-warm-panel border border-warm-border rounded p-4">
      <h2 class="text-warm-text font-bold mb-3">Run History</h2>
      {#if runsError}
        <div class="text-xs text-[#B85C4A] mb-2">{runsError}</div>
      {/if}
      {#if loadingRuns}
        <div class="text-xs text-warm-muted">Loading…</div>
      {:else if runs.length === 0}
        <div class="text-xs text-warm-muted">No run history yet</div>
      {:else}
        <div class="border border-warm-border rounded overflow-hidden">
          <table class="w-full text-xs">
            <thead class="bg-warm-bg">
              <tr>
                <th class="text-left px-3 py-2 text-warm-sub">Pipeline</th>
                <th class="text-left px-3 py-2 text-warm-sub">Status</th>
                <th class="text-left px-3 py-2 text-warm-sub">Duration</th>
                <th class="text-left px-3 py-2 text-warm-sub">Submitted</th>
              </tr>
            </thead>
            <tbody>
              {#each runs as run (run.job_id)}
                <tr class="bg-white border-t border-warm-border cursor-pointer hover:bg-warm-light/40" onclick={() => void toggleRow(run.job_id)}>
                  <td class="px-3 py-2 text-warm-text">{run.pipeline_id} v{run.pipeline_version}</td>
                  <td class="px-3 py-2">
                    <span
                      class="inline-flex items-center rounded px-2 py-0.5"
                      class:bg-[#E7F4EA]={run.status === 'Completed'}
                      class:text-[#2E6A3E]={run.status === 'Completed'}
                      class:bg-[#FCE8E5]={run.status === 'Failed'}
                      class:text-[#8A3F31]={run.status === 'Failed'}
                      class:bg-warm-bg={run.status !== 'Completed' && run.status !== 'Failed'}
                      class:text-warm-sub={run.status !== 'Completed' && run.status !== 'Failed'}
                    >{run.status}</span>
                  </td>
                  <td class="px-3 py-2 text-warm-sub">{run.duration_secs === null ? '—' : `${run.duration_secs.toFixed(2)}s`}</td>
                  <td class="px-3 py-2 text-warm-sub">{new Date(run.submitted_at).toLocaleString()}</td>
                </tr>
                {#if expandedJobId === run.job_id}
                  <tr class="bg-warm-bg border-t border-warm-border">
                    <td colspan="4" class="px-3 py-2">
                      {#if run.error}
                        <div class="text-xs text-[#B85C4A] mb-2">{run.error}</div>
                      {/if}
                      {#if jobDetails[run.job_id]?.node_progress?.length}
                        <div class="flex flex-col gap-1">
                          {#each jobDetails[run.job_id].node_progress as node (node.node_id)}
                            <div class="text-xs text-warm-sub">{node.node_id} — {node.status}{node.rows_processed !== null ? ` (${node.rows_processed} rows)` : ''}</div>
                          {/each}
                        </div>
                      {:else}
                        <div class="text-xs text-warm-muted">No node details</div>
                      {/if}
                    </td>
                  </tr>
                {/if}
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>
  </div>
</div>
