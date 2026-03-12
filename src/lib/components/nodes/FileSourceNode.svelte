<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';
  import { open } from '@tauri-apps/plugin-dialog';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  let filePath = $state(data.config?.path || '');
  let detectedType = $derived(detectType(filePath));

  function detectType(p: string): string {
    const ext = p.split('.').pop()?.toLowerCase() ?? '';
    if (ext === 'csv') return 'CSV';
    if (ext === 'xlsx' || ext === 'xls') return 'XLSX';
    if (ext === 'parquet') return 'Parquet';
    return '';
  }

  function statusRingClass() {
    const s = data?.run_state;
    if (s === 'running') return 'border-accent animate-pulse bg-accent-light';
    if (s === 'done') return 'border-[#4A7C59] bg-[#4A7C59]';
    if (s === 'error') return 'border-[#B85C4A] bg-[#B85C4A]';
    return 'border-warm-border bg-white';
  }

  function updateConfig() {
    data.config = { path: filePath };
  }

  function stopFlowEvents(e: Event) {
    e.stopPropagation();
  }

  async function removeNode(e: Event) {
    e.stopPropagation();
    await deleteElements({ nodes: [{ id }] });
  }

  async function browse(e: Event) {
    e.stopPropagation();
    const selected = await open({
      multiple: false,
      filters: [
        { name: 'Supported Files', extensions: ['csv', 'xlsx', 'xls', 'parquet'] },
        { name: 'CSV', extensions: ['csv'] },
        { name: 'Excel', extensions: ['xlsx', 'xls'] },
        { name: 'Parquet', extensions: ['parquet'] },
      ],
    });
    if (typeof selected === 'string') {
      filePath = selected;
      updateConfig();
    }
  }

  $effect(() => {
    if (data.config?.path !== undefined && data.config.path !== filePath) {
      filePath = data.config.path;
    }
  });
</script>

<div class="bg-white border-l-4 border-l-[#7A5C9C] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">📂</span>
      <span class="font-bold text-warm-text text-sm">File Source</span>
      {#if detectedType}
        <span class="text-[10px] px-1.5 py-0.5 rounded bg-[#7A5C9C]/10 text-[#7A5C9C] font-semibold">{detectedType}</span>
      {/if}
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

  <div class="nodrag p-3 flex flex-col gap-3 relative">
    <div>
      <label for="file-path-{id}" class="text-xs text-warm-sub font-medium">File Path</label>
      <div class="flex gap-1 mt-0.5">
        <input
          id="file-path-{id}"
          type="text"
          bind:value={filePath}
          oninput={updateConfig}
          placeholder="Select a file or paste path…"
          class="flex-1 min-w-0 text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none font-mono truncate"
        />
        <button
          type="button"
          onclick={browse}
          onpointerdown={stopFlowEvents}
          class="nodrag shrink-0 text-xs px-2 py-1 border border-warm-border rounded hover:bg-warm-light text-warm-sub transition-colors"
        >
          Browse
        </button>
      </div>
      {#if filePath && !detectType(filePath)}
        <div class="mt-1 text-[10px] text-[#B85C4A]">Unsupported extension — use .csv, .xlsx, .xls, or .parquet</div>
      {:else if filePath}
        <div class="mt-1 text-[10px] text-warm-muted truncate">{filePath.split('/').pop()}</div>
      {/if}
    </div>

    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}

    <Handle type="source" position={Position.Right} class="!bg-[#7A5C9C] !w-3 !h-3 !-right-1.5" />
  </div>
</div>
