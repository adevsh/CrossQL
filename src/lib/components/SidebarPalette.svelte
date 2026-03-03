<script lang="ts">
  import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
  import { executionStore } from '$lib/stores/executionStore.svelte';

  function addNode(type: Parameters<typeof pipelineStore.addNode>[0]) {
    pipelineStore.addNode(type);
  }
</script>

<aside class="w-64 bg-warm-panel border-r border-warm-border p-4 flex flex-col">
  <h2 class="text-warm-text font-bold mb-4">Node Palette</h2>
  <div class="flex flex-col gap-4">
    <div>
      <div class="text-xs text-warm-sub font-semibold mb-2">Sources</div>
      <div class="flex flex-col gap-2">
        <button
          type="button"
          onclick={() => addNode('postgres')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🐘</span><span>PostgreSQL</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('mysql')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🐬</span><span>MySQL</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('mongodb')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🍃</span><span>MongoDB</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('cassandra')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>👁</span><span>Cassandra</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
      </div>
    </div>

    <div>
      <div class="text-xs text-warm-sub font-semibold mb-2">Outputs</div>
      <div class="flex flex-col gap-2">
        <button
          type="button"
          onclick={() => addNode('parquet')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>📦</span><span>Parquet</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
      </div>
    </div>

    <div>
      <div class="text-xs text-warm-sub font-semibold mb-2">Transforms</div>
      <div class="flex flex-col gap-2">
        <button
          type="button"
          onclick={() => addNode('schema_map')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🔀</span><span>Schema Map</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('join')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🎲</span><span>Join</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('filter')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🔽</span><span>Filter</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('select')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>✅</span><span>Select</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('rename')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>✏️</span><span>Rename</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('cast')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🧬</span><span>Cast</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('derived')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>➕</span><span>Derived</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
      </div>
    </div>
  </div>
  
  <div class="mt-auto border-t border-warm-border pt-4">
    <button 
      onclick={executionStore.startRun}
      class="w-full px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
      disabled={executionStore.runState === 'running'}
    >
      Run Pipeline
    </button>
    <button
      type="button"
      onclick={executionStore.cancelRun}
      class="w-full mt-2 px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
      disabled={!executionStore.runId}
    >
      Cancel
    </button>
    <div class="text-xs mt-2">
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
    <div class="mt-3 border border-warm-border rounded bg-warm-bg">
      <div class="px-3 py-2 text-xs font-semibold text-warm-sub border-b border-warm-border">
        Execution Log
      </div>
      <div class="max-h-44 overflow-auto px-3 py-2">
        {#if executionStore.runLogs.length === 0}
          <div class="text-xs text-warm-muted">No logs yet</div>
        {:else}
          <div class="flex flex-col gap-1">
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
</aside>
