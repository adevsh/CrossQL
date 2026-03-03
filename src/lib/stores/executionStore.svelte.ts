import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { formatBytes } from '$lib/utils';
import { pipelineStore } from './pipelineStore.svelte';

export type PipelineEvent = {
  run_id: string;
  kind: string;
  node_id?: string | null;
  state?: string | null;
  message?: string | null;
  result?: any | null;
};

function createExecutionStore() {
  let invokeResult = $state('Idle');
  let runState = $state<'idle' | 'running' | 'success' | 'error'>('idle');
  let runRowCount = $state<number | null>(null);
  let runFileSizeBytes = $state<number | null>(null);
  let runOutputPath = $state<string | null>(null);
  let runId = $state<string | null>(null);
  let runLogs = $state<Array<{ ts: number; message: string }>>([]);

  // Process usage
  let usageState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let usageError = $state<string | null>(null);
  let usageCpuPercent = $state<number | null>(null);
  let usageMemoryBytes = $state<number | null>(null);

  function appendLog(message: string) {
    runLogs = [...runLogs, { ts: Date.now(), message }].slice(-300);
  }

  async function loadUsage() {
    usageState = 'loading';
    usageError = null;
    try {
      const result = await invoke('get_process_usage');
      const r = result as any;
      usageCpuPercent = typeof r?.cpu_percent === 'number' ? r.cpu_percent : null;
      usageMemoryBytes = typeof r?.memory_bytes === 'number' ? r.memory_bytes : null;
      usageState = 'ready';
    } catch (e) {
      usageState = 'error';
      usageError = `${e}`;
      usageCpuPercent = null;
      usageMemoryBytes = null;
    }
  }

  async function waitForRun(id: string) {
    try {
      const result = await invoke('await_pipeline_run', { runId: id });
      if (runId !== id) return;
      const r = result as any;
      runState = 'success';
      if (typeof r?.row_count === 'number' && typeof r?.file_size_bytes === 'number' && typeof r?.path === 'string') {
        runRowCount = r.row_count;
        runFileSizeBytes = r.file_size_bytes;
        runOutputPath = r.path;
        invokeResult = `Success: ${r.row_count} rows → ${r.path} (${formatBytes(r.file_size_bytes)})`;
      } else {
        invokeResult = 'Success';
      }
      if (Array.isArray(r?.node_stats)) {
        pipelineStore.applyNodeStats(r.node_stats);
      }
      pipelineStore.setAllNodeRunState('done');
      appendLog('Run finished');
      runId = null;
    } catch (e: any) {
      if (runId !== id) return;
      runState = 'error';
      invokeResult = `Error: ${e?.toString?.() ?? e}`;
      pipelineStore.setAllNodeRunState('error', e?.toString?.() ?? `${e}`);
      appendLog(`Error: ${e?.toString?.() ?? e}`);
      runId = null;
    }
  }

  async function cancelRun() {
    if (!runId) return;
    invokeResult = 'Cancelling…';
    try {
      await invoke('cancel_pipeline_run', { runId });
      appendLog('Cancel requested');
    } catch (e: any) {
      runState = 'error';
      invokeResult = `Error: ${e?.toString?.() ?? e}`;
    }
  }

  async function startRun() {
    runState = 'running';
    runRowCount = null;
    runFileSizeBytes = null;
    runOutputPath = null;

    const { nodes, edges } = pipelineStore;

    const outputNodes = nodes.filter((n: any) => n.type === 'parquet');
    if (outputNodes.length !== 1) {
      runState = 'error';
      invokeResult = "Error: Exactly one Parquet Output node is required";
      return;
    }

    const outputNode = outputNodes[0];
    if (!outputNode?.data?.config) {
      runState = 'error';
      invokeResult = "Error: Missing output node config";
      return;
    }

    const output = outputNode.data.config as any;

    for (const n of nodes) {
      if (n.type !== 'postgres' && n.type !== 'mysql' && n.type !== 'mongodb' && n.type !== 'cassandra' && n.type !== 'join') continue;
      if (!n?.data?.config) {
        runState = 'error';
        invokeResult = `Error: Missing config for node ${n.id}`;
        return;
      }

      const cfg = n.data.config as any;

      if (n.type === 'mongodb') {
        if (!cfg.uri) { runState = 'error'; invokeResult = "Error: MongoDB 'uri' is required"; return; }
        if (!cfg.database) { runState = 'error'; invokeResult = "Error: MongoDB 'database' is required"; return; }
        if (!cfg.collection) { runState = 'error'; invokeResult = "Error: MongoDB 'collection' is required"; return; }
      } else if (n.type === 'cassandra') {
        if (!cfg.contact_points) { runState = 'error'; invokeResult = "Error: Cassandra 'contact_points' is required"; return; }
        if (!cfg.keyspace) { runState = 'error'; invokeResult = "Error: Cassandra 'keyspace' is required"; return; }
        if (!cfg.query) { runState = 'error'; invokeResult = "Error: Cassandra 'query' is required"; return; }
      } else if (n.type === 'join') {
        if (!cfg.left_on || !cfg.right_on) { runState = 'error'; invokeResult = "Error: Join requires left_on and right_on"; return; }
      } else {
        if (!cfg.host) { runState = 'error'; invokeResult = "Error: Source 'host' is required"; return; }
        if (!cfg.port) { runState = 'error'; invokeResult = "Error: Source 'port' is required"; return; }
        if (!cfg.user) { runState = 'error'; invokeResult = "Error: Source 'user' is required"; return; }
        if (cfg.password === undefined || cfg.password === null) { runState = 'error'; invokeResult = "Error: Source 'password' is required"; return; }
        if (!cfg.database) { runState = 'error'; invokeResult = "Error: Source 'database' is required"; return; }
        if (!cfg.query) { runState = 'error'; invokeResult = "Error: Source 'query' is required"; return; }
      }
    }

    if (!output.path) {
      runState = 'error';
      invokeResult = "Error: Output path is required";
      return;
    }

    pipelineStore.setAllNodeRunState('running');
    runLogs = [];
    appendLog('Starting run…');
    invokeResult = "Starting run...";
    try {
      const id = await invoke('start_pipeline_run', { nodes, edges });
      runId = typeof id === 'string' ? id : id ? `${id}` : null;
      if (runId) appendLog(`Run id: ${runId}`);
      invokeResult = "Running pipeline...";
      if (runId) void waitForRun(runId);
    } catch (e) {
      runState = 'error';
      invokeResult = `Error: ${e}`;
    }
  }

  /**
   * Setup the Tauri event listener for pipeline_event.
   * Call inside onMount; returns an unlisten function.
   */
  function setupEventListener(): Promise<() => void> {
    return listen<PipelineEvent>('pipeline_event', (event) => {
      const p = event.payload;
      if (!p) return;

      if (p.kind === 'run_started') {
        runId = p.run_id;
        runState = 'running';
        invokeResult = 'Running…';
        runRowCount = null;
        runFileSizeBytes = null;
        runOutputPath = null;
        runLogs = [];
        pipelineStore.setAllNodeRunState('running');
        appendLog('Run started');
        return;
      }

      if (runId && p.run_id !== runId) return;

      if (p.kind === 'node_state' && p.node_id) {
        pipelineStore.updateNodeData(p.node_id, { run_state: p.state ?? 'idle', run_error: p.message ?? null });
        if (p.state) appendLog(`${p.node_id}: ${p.state}`);
        return;
      }

      if (p.kind === 'run_finished' && p.result) {
        runState = 'success';
        if (typeof p.result?.row_count === 'number' && typeof p.result?.file_size_bytes === 'number' && typeof p.result?.path === 'string') {
          const rowCount = p.result.row_count as number;
          const fileSizeBytes = p.result.file_size_bytes as number;
          const outPath = p.result.path as string;
          runRowCount = rowCount;
          runFileSizeBytes = fileSizeBytes;
          runOutputPath = outPath;
          invokeResult = `Success: ${rowCount} rows → ${outPath} (${formatBytes(fileSizeBytes)})`;
        } else {
          invokeResult = 'Success';
        }
        if (Array.isArray(p.result.node_stats)) {
          pipelineStore.applyNodeStats(p.result.node_stats);
        }
        pipelineStore.setAllNodeRunState('done');
        appendLog('Run finished');
        runId = null;
        return;
      }

      if (p.kind === 'run_cancelled') {
        runState = 'idle';
        invokeResult = 'Cancelled';
        pipelineStore.setAllNodeRunState('idle');
        appendLog('Run cancelled');
        runId = null;
        return;
      }

      if (p.kind === 'run_error') {
        runState = 'error';
        invokeResult = `Error: ${p.message ?? 'Unknown error'}`;
        pipelineStore.setAllNodeRunState('error', p.message ?? 'Unknown error');
        appendLog(`Error: ${p.message ?? 'Unknown error'}`);
        runId = null;
      }
    });
  }

  return {
    get invokeResult() { return invokeResult; },
    get runState() { return runState; },
    get runRowCount() { return runRowCount; },
    get runFileSizeBytes() { return runFileSizeBytes; },
    get runOutputPath() { return runOutputPath; },
    get runId() { return runId; },
    get runLogs() { return runLogs; },
    get usageState() { return usageState; },
    get usageError() { return usageError; },
    get usageCpuPercent() { return usageCpuPercent; },
    get usageMemoryBytes() { return usageMemoryBytes; },
    startRun,
    cancelRun,
    loadUsage,
    setupEventListener,
  };
}

export const executionStore = createExecutionStore();
