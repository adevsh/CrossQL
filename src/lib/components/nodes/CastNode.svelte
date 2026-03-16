<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  type Cast = { column: string; dtype: 'Int64' | 'Float64' | 'Boolean' | 'Utf8' | 'Datetime' };

  let casts = $state<Cast[]>([]);

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

  function updateConfig() {
    data.config = { casts };
  }

  function addCast() {
    casts = [...casts, { column: '', dtype: 'Utf8' }];
    updateConfig();
  }

  function removeCast(idx: number) {
    casts = casts.filter((_, i) => i !== idx);
    updateConfig();
  }

  $effect(() => {
    const nextCasts = Array.isArray(data?.config?.casts) ? (data.config.casts as Cast[]) : [];
    if (casts !== nextCasts) casts = nextCasts;
  });
</script>

<div class="bg-white border-l-4 border-l-[#C49A3C] border border-warm-border rounded shadow-sm w-[28rem]">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">🧬</span>
      <span class="font-bold text-warm-text text-sm">Cast</span>
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

  <div class="nodrag p-3 flex flex-col gap-3 relative" role="group" onpointerdown={stopFlowEvents} onwheel={stopFlowEvents}>
    <Handle type="target" position={Position.Left} class="!bg-[#C49A3C] !w-3 !h-3 !-left-1.5" />
    <Handle type="source" position={Position.Right} class="!bg-[#C49A3C] !w-3 !h-3" />

    {#if casts.length === 0}
      <div class="text-xs text-warm-muted">Add one or more casts.</div>
    {:else}
      <div class="border border-warm-border rounded overflow-hidden">
        <div class="grid grid-cols-12 gap-0 bg-warm-panel text-[11px] text-warm-sub font-semibold border-b border-warm-border">
          <div class="col-span-6 px-2 py-1">Column</div>
          <div class="col-span-5 px-2 py-1">Type</div>
          <div class="col-span-1 px-2 py-1"></div>
        </div>
        <div class="max-h-56 overflow-auto">
          {#each casts as c, i (i)}
            <div class="grid grid-cols-12 gap-0 border-b border-warm-border last:border-b-0 bg-white">
              <div class="col-span-6 p-1">
                <input
                  type="text"
                  bind:value={c.column}
                  oninput={updateConfig}
                  onpointerdown={stopFlowEvents}
                  placeholder="column_name"
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                />
              </div>
              <div class="col-span-5 p-1">
                <select
                  bind:value={c.dtype}
                  onchange={updateConfig}
                  onpointerdown={stopFlowEvents}
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
                >
                  <option value="Int64">Int64</option>
                  <option value="Float64">Float64</option>
                  <option value="Boolean">Boolean</option>
                  <option value="Utf8">Utf8</option>
                  <option value="Datetime">Datetime</option>
                </select>
              </div>
              <div class="col-span-1 p-1 flex items-center justify-end">
                <button
                  type="button"
                  onclick={() => removeCast(i)}
                  onpointerdown={stopFlowEvents}
                  class="text-xs px-2 py-1 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
                  aria-label="Remove"
                >
                  ×
                </button>
              </div>
            </div>
          {/each}
        </div>
      </div>
    {/if}

    <button
      type="button"
      onclick={addCast}
      onpointerdown={stopFlowEvents}
      class="self-start text-xs px-3 py-1.5 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
    >
      Add cast
    </button>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>
</div>
