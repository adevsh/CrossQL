<script lang="ts">
  import TypeBadge from '$lib/components/TypeBadge.svelte';
  import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
  import { schemaStore } from '$lib/stores/schemaStore.svelte';
  import { executionStore } from '$lib/stores/executionStore.svelte';
  import { formatBytes } from '$lib/utils';
  import { invoke } from '@tauri-apps/api/core';

  let querySql = $state('SELECT * FROM result LIMIT 50');
  let queryColumns = $state<string[]>([]);
  let queryRows = $state<any[][]>([]);
  let queryState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let queryError = $state('');
  let queryTotalRows = $state(0);

  // Reset query results when the selected node or output file changes
  $effect(() => {
    const _ = pipelineStore.selectedNodeId;
    const __ = executionStore.runOutputPath;
    queryState = 'idle';
    queryColumns = [];
    queryRows = [];
    queryTotalRows = 0;
    queryError = '';
  });

  async function runQuery() {
    const sel = pipelineStore.nodes.find((n: any) => n.id === pipelineStore.selectedNodeId);
    if (!sel || sel.type !== 'parquet' || !sel.data?.config?.path) return;
    queryState = 'loading';
    queryError = '';
    try {
      const result = await invoke<any>('query_parquet', { path: sel.data.config.path, sql: querySql });
      queryColumns = result.columns;
      queryRows = result.rows;
      queryTotalRows = result.total_rows;
      queryState = 'ready';
    } catch (e: any) {
      queryError = e?.toString?.() ?? `${e}`;
      queryState = 'error';
    }
  }
</script>

<aside class="w-80 bg-warm-panel border-l border-warm-border p-4 flex flex-col">
  <h2 class="text-warm-text font-bold mb-4">Configuration</h2>
  <div class="flex-1 overflow-auto">
    {#if pipelineStore.selectedNodeId}
      {@const selectedNode = pipelineStore.nodes.find((n: any) => n.id === pipelineStore.selectedNodeId)}
      {#if selectedNode}
        <div class="text-sm text-warm-text font-medium mb-1">
          {selectedNode.type} ({selectedNode.id})
        </div>
        {#if selectedNode.type === 'postgres' || selectedNode.type === 'mysql' || selectedNode.type === 'mongodb' || selectedNode.type === 'cassandra' || selectedNode.type === 'schema_map'}
          <div class="text-xs text-warm-sub mb-2">
            {selectedNode.type === 'schema_map' ? 'Schema (after map)' : 'Schema'}
          </div>
          {#if schemaStore.schemaState === 'loading'}
            <div class="text-xs text-warm-muted">Loading…</div>
          {:else if schemaStore.schemaState === 'error'}
            <div class="text-xs text-[#B85C4A]">{schemaStore.schemaError}</div>
          {:else if schemaStore.schemaState === 'ready'}
            {#if schemaStore.schemaFields.length === 0}
              <div class="text-xs text-warm-muted">No columns</div>
            {:else}
              <div class="flex flex-col gap-2">
                {#each schemaStore.schemaFields as f (f.name)}
                  <div class="flex items-center justify-between gap-3">
                    <div class="text-xs text-warm-text truncate">{f.name}</div>
                    <TypeBadge dtype={f.dtype} />
                  </div>
                {/each}
              </div>
            {/if}
          {:else}
            <div class="text-xs text-warm-muted">Select a source node to preview schema</div>
          {/if}
        {:else if selectedNode.type === 'join'}
          <div class="text-xs text-warm-sub mb-2">Preview (first 50 rows)</div>
          {#if schemaStore.previewState === 'loading'}
            <div class="text-xs text-warm-muted">Loading…</div>
          {:else if schemaStore.previewState === 'error'}
            <div class="text-xs text-[#B85C4A]">{schemaStore.previewError}</div>
          {:else if schemaStore.previewState === 'ready'}
            {#if schemaStore.previewColumns.length === 0}
              <div class="text-xs text-warm-muted">No columns</div>
            {:else}
              <div class="border border-warm-border rounded overflow-auto">
                <table class="w-full text-xs">
                  <thead class="bg-warm-bg sticky top-0">
                    <tr>
                      {#each schemaStore.previewColumns as c (c)}
                        <th class="text-left px-2 py-1 border-b border-warm-border font-semibold text-warm-sub whitespace-nowrap">{c}</th>
                      {/each}
                    </tr>
                  </thead>
                  <tbody>
                    {#each schemaStore.previewRows as r, i (i)}
                      <tr class="odd:bg-white even:bg-warm-panel/40">
                        {#each schemaStore.previewColumns as c (c)}
                          <td class="px-2 py-1 border-b border-warm-border text-warm-text whitespace-nowrap">
                            {r?.[c] === null || r?.[c] === undefined ? '—' : `${r[c]}`}
                          </td>
                        {/each}
                      </tr>
                    {/each}
                  </tbody>
                </table>
              </div>
            {/if}
          {:else}
            <div class="text-xs text-warm-muted">Select a Join node to preview</div>
          {/if}
        {:else}
          <div class="text-xs text-warm-muted">No schema preview for this node type</div>
        {/if}
      {:else}
        <div class="text-warm-muted text-sm italic">Select a node</div>
      {/if}
    {:else}
      <div class="text-warm-muted text-sm italic">
        Select a node to see its schema
      </div>
    {/if}
  </div>

  <!-- Parquet SQL Query — only shown when this file has been written by a successful run -->
  {#if pipelineStore.selectedNodeId}
    {@const selNode = pipelineStore.nodes.find((n: any) => n.id === pipelineStore.selectedNodeId)}
    {@const nodePath = selNode?.data?.config?.path}
    {@const fileReady = selNode?.type === 'parquet' && nodePath && executionStore.runOutputPath === nodePath}
    {#if fileReady}
      <div class="border-t border-warm-border pt-3 mt-3">
        <div class="flex items-center justify-between mb-2">
          <div class="text-xs text-warm-sub font-semibold">Query Output</div>
          <div class="text-[10px] text-[#4A7C59]">✓ File ready</div>
        </div>
        <div class="text-[10px] text-warm-muted mb-1">Table: <code class="text-warm-sub">result</code></div>
        <textarea
          bind:value={querySql}
          rows="2"
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-[#C07A3A] outline-none font-mono bg-white resize-y"
          placeholder="SELECT * FROM result LIMIT 50"
        ></textarea>
        <button
          onclick={() => void runQuery()}
          class="mt-1 w-full px-3 py-1.5 text-xs bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors"
          disabled={queryState === 'loading'}
        >
          {queryState === 'loading' ? 'Querying…' : '▶ Run Query'}
        </button>
        {#if queryState === 'error'}
          <div class="mt-1 text-xs text-[#B85C4A]">{queryError}</div>
        {/if}
        {#if queryState === 'ready'}
          <div class="mt-2 text-[10px] text-warm-muted">{queryTotalRows} row{queryTotalRows === 1 ? '' : 's'}</div>
          {#if queryColumns.length > 0}
            <div class="mt-1 border border-warm-border rounded overflow-auto max-h-48">
              <table class="w-full text-xs">
                <thead class="bg-warm-bg sticky top-0">
                  <tr>
                    {#each queryColumns as c (c)}
                      <th class="text-left px-2 py-1 border-b border-warm-border font-semibold text-warm-sub whitespace-nowrap">{c}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each queryRows as row, i (i)}
                    <tr class="odd:bg-white even:bg-warm-panel/40">
                      {#each row as val, j (j)}
                        <td class="px-2 py-1 border-b border-warm-border text-warm-text whitespace-nowrap">
                          {val === null ? '—' : `${val}`}
                        </td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {/if}
        {/if}
      </div>
    {/if}
  {/if}

  <!-- Execution Controls -->
  <div class="border-t border-warm-border pt-3 mt-4 space-y-2">
    <div class="flex gap-2">
      <button 
        onclick={executionStore.startRun}
        class="flex-1 px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
        disabled={executionStore.runState === 'running'}
      >
        Run Pipeline
      </button>
      <button
        type="button"
        onclick={executionStore.cancelRun}
        class="px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
        disabled={!executionStore.runId}
      >
        Cancel
      </button>
    </div>
    <div class="text-xs">
      {#if executionStore.runState === 'running'}
        <div class="text-warm-sub">Running…</div>
      {:else if executionStore.runState === 'success'}
        <div class="text-[#4A7C59]">{executionStore.invokeResult}</div>
      {:else if executionStore.runState === 'error'}
        <div class="text-[#B85C4A]">{executionStore.invokeResult}</div>
      {:else}
        <div class="text-warm-muted">{executionStore.invokeResult}</div>
      {/if}
    </div>
    <div class="border border-warm-border rounded bg-warm-bg">
      <div class="px-3 py-1.5 text-xs font-semibold text-warm-sub border-b border-warm-border">
        Execution Log
      </div>
      <div class="max-h-[6.5rem] overflow-auto px-3 py-2">
        {#if executionStore.runLogs.length === 0}
          <div class="text-xs text-warm-muted">No logs yet</div>
        {:else}
          <div class="flex flex-col gap-0.5">
            {#each executionStore.runLogs as l (l.ts)}
              <div class="text-[11px] text-warm-sub">
                {new Date(l.ts).toLocaleTimeString()} — {l.message}
              </div>
            {/each}
          </div>
        {/if}
      </div>
    </div>
  </div>

  <div class="mt-3 border-t border-warm-border pt-3">
    <div class="text-xs text-warm-sub font-semibold mb-2">App Usage</div>
    {#if executionStore.usageState === 'loading'}
      <div class="text-xs text-warm-muted">Loading…</div>
    {:else if executionStore.usageState === 'error'}
      <div class="text-xs text-[#B85C4A] truncate">{executionStore.usageError}</div>
    {:else if executionStore.usageState === 'ready'}
      <div class="flex flex-col gap-1 text-xs text-warm-text">
        <div class="flex items-center justify-between">
          <div class="text-warm-sub">CPU</div>
          <div>{executionStore.usageCpuPercent === null ? '—' : `${executionStore.usageCpuPercent.toFixed(1)}%`}</div>
        </div>
        <div class="flex items-center justify-between">
          <div class="text-warm-sub">Memory</div>
          <div>{executionStore.usageMemoryBytes === null ? '—' : formatBytes(executionStore.usageMemoryBytes)}</div>
        </div>
        <div class="text-[11px] text-warm-muted">Refreshes every 5s</div>
      </div>
    {:else}
      <div class="text-xs text-warm-muted">Idle</div>
    {/if}
  </div>
</aside>
