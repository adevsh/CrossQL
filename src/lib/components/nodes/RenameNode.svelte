<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  type Mapping = { from: string; to: string };

  let mappings = $state<Mapping[]>((data.config?.mappings as Mapping[] | undefined) ?? []);

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
    data.config = { mappings };
  }

  function addMapping() {
    mappings = [...mappings, { from: '', to: '' }];
    updateConfig();
  }

  function removeMapping(idx: number) {
    mappings = mappings.filter((_, i) => i !== idx);
    updateConfig();
  }
</script>

<div class="bg-white border-l-4 border-l-[#C49A3C] border border-warm-border rounded shadow-sm w-[28rem]">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">✏️</span>
      <span class="font-bold text-warm-text text-sm">Rename</span>
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

    {#if mappings.length === 0}
      <div class="text-xs text-warm-muted">Add one or more rename mappings.</div>
    {:else}
      <div class="border border-warm-border rounded overflow-hidden">
        <div class="grid grid-cols-12 gap-0 bg-warm-panel text-[11px] text-warm-sub font-semibold border-b border-warm-border">
          <div class="col-span-5 px-2 py-1">From</div>
          <div class="col-span-6 px-2 py-1">To</div>
          <div class="col-span-1 px-2 py-1"></div>
        </div>
        <div class="max-h-56 overflow-auto">
          {#each mappings as m, i (i)}
            <div class="grid grid-cols-12 gap-0 border-b border-warm-border last:border-b-0 bg-white">
              <div class="col-span-5 p-1">
                <input
                  type="text"
                  bind:value={m.from}
                  oninput={updateConfig}
                  onpointerdown={stopFlowEvents}
                  placeholder="old_name"
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                />
              </div>
              <div class="col-span-6 p-1">
                <input
                  type="text"
                  bind:value={m.to}
                  oninput={updateConfig}
                  onpointerdown={stopFlowEvents}
                  placeholder="new_name"
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                />
              </div>
              <div class="col-span-1 p-1 flex items-center justify-end">
                <button
                  type="button"
                  onclick={() => removeMapping(i)}
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
      onclick={addMapping}
      onpointerdown={stopFlowEvents}
      class="self-start text-xs px-3 py-1.5 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
    >
      Add mapping
    </button>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>
</div>
