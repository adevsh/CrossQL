<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';
  import { save } from '@tauri-apps/plugin-dialog';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  let path = $state('');
  let compression = $state('snappy');
  let rowGroupSize = $state(524288);

  function updateConfig() {
    data.config = { path, compression, row_group_size: rowGroupSize };
  }

  async function browseFile() {
    const selected = await save({
      filters: [{
        name: 'Parquet',
        extensions: ['parquet']
      }]
    });
    if (selected) {
      path = selected;
      updateConfig();
    }
  }

  function stopFlowEvents(e: Event) {
    e.stopPropagation();
  }

  async function removeNode(e: Event) {
    e.stopPropagation();
    await deleteElements({ nodes: [{ id }] });
  }

  function statusRingClass() {
    const s = data?.run_state;
    if (s === 'running') return 'border-accent animate-pulse bg-accent-light';
    if (s === 'done') return 'border-[#4A7C59] bg-[#4A7C59]';
    if (s === 'error') return 'border-[#B85C4A] bg-[#B85C4A]';
    return 'border-warm-border bg-white';
  }

  $effect(() => {
    const cfg = data?.config ?? {};
    const nextPath = typeof cfg.path === 'string' ? cfg.path : '';
    const nextCompression = typeof cfg.compression === 'string' ? cfg.compression : 'snappy';
    const nextRowGroupSize =
      typeof cfg.row_group_size === 'number' && Number.isFinite(cfg.row_group_size)
        ? cfg.row_group_size
        : 524288;
    if (path !== nextPath) path = nextPath;
    if (compression !== nextCompression) compression = nextCompression;
    if (rowGroupSize !== nextRowGroupSize) rowGroupSize = nextRowGroupSize;
  });
</script>

<div class="bg-white border-l-4 border-l-[#4A7C59] border border-warm-border rounded shadow-sm w-72">
  <!-- Header -->
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">📦</span>
      <span class="font-bold text-warm-text text-sm">Parquet Output</span>
    </div>
    <button
      type="button"
      onclick={removeNode}
      onpointerdown={stopFlowEvents}
      class="nodrag w-6 h-6 flex items-center justify-center rounded hover:bg-warm-light text-warm-sub"
      aria-label="Remove node"
      title="Remove"
    >
      ×
    </button>
  </div>

  <!-- Body -->
  <div class="nodrag p-3 flex flex-col gap-3 relative">
    <!-- Input Handle -->
    <Handle type="target" position={Position.Left} class="!bg-[#4A7C59] !w-3 !h-3 !-left-1.5" />

    <div>
      <label for="output-path-{id}" class="text-xs text-warm-sub font-medium">Output Path</label>
      <div class="flex gap-1">
        <input 
          id="output-path-{id}"
          type="text" 
          bind:value={path} 
          oninput={updateConfig}
          class="flex-1 text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
        <button 
          onclick={browseFile}
          class="text-xs px-2 py-1 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
        >
          ...
        </button>
      </div>
    </div>

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label for="output-compression-{id}" class="text-xs text-warm-sub font-medium">Compression</label>
        <select 
          id="output-compression-{id}"
          bind:value={compression} 
          onchange={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
        >
          <option value="snappy">Snappy</option>
          <option value="zstd">Zstd</option>
          <option value="gzip">Gzip</option>
          <option value="lz4">Lz4</option>
          <option value="none">None</option>
        </select>
      </div>
      <div>
        <label for="output-row-group-{id}" class="text-xs text-warm-sub font-medium">Row Group</label>
        <input 
          id="output-row-group-{id}"
          type="number" 
          bind:value={rowGroupSize} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>
</div>
