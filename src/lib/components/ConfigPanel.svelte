<script lang="ts">
  import TypeBadge from '$lib/components/TypeBadge.svelte';
  import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
  import { schemaStore } from '$lib/stores/schemaStore.svelte';
  import { executionStore } from '$lib/stores/executionStore.svelte';
  import { formatBytes } from '$lib/utils';
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

  <div class="mt-4 border-t border-warm-border pt-3">
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
