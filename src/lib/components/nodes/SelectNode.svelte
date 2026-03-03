<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  let columnsText = $state<string>((data.config?.columns ?? []).join(', '));

  function parseColumns(text: string) {
    return text
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
  }

  function updateConfig() {
    data.config = { columns: parseColumns(columnsText) };
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
</script>

<div class="bg-white border-l-4 border-l-[#C49A3C] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">✅</span>
      <span class="font-bold text-warm-text text-sm">Select Columns</span>
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

  <div class="nodrag p-3 flex flex-col gap-3 relative" onpointerdown={stopFlowEvents} onwheel={stopFlowEvents}>
    <Handle type="target" position={Position.Left} class="!bg-[#C49A3C] !w-3 !h-3 !-left-1.5" />
    <Handle type="source" position={Position.Right} class="!bg-[#C49A3C] !w-3 !h-3" />

    <div>
      <label class="text-xs text-warm-sub font-medium">Columns (comma separated)</label>
      <textarea
        bind:value={columnsText}
        oninput={updateConfig}
        onpointerdown={stopFlowEvents}
        placeholder="e.g. id, city, region"
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
      <div class="text-[11px] text-warm-muted mt-1">Keeps only the listed columns.</div>
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>
</div>
