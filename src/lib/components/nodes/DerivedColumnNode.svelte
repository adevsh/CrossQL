<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  type Op = 'upper' | 'lower' | 'add' | 'concat';
  type RightKind = 'column' | 'literal';

  let name = $state<string>(data.config?.name || 'derived');
  let op = $state<Op>(data.config?.op || 'upper');
  let left = $state<string>(data.config?.left || '');
  let rightKind = $state<RightKind>(data.config?.right_kind || 'column');
  let right = $state<string>(data.config?.right || '');

  function updateConfig() {
    data.config = { name, op, left, right_kind: rightKind, right };
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

<div class="bg-white border-l-4 border-l-[#C49A3C] border border-warm-border rounded shadow-sm w-[28rem]">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">➕</span>
      <span class="font-bold text-warm-text text-sm">Derived Column</span>
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

    <div class="grid grid-cols-2 gap-2">
      <div class="col-span-2">
        <label class="text-xs text-warm-sub font-medium">New Column Name</label>
        <input
          type="text"
          bind:value={name}
          oninput={updateConfig}
          onpointerdown={stopFlowEvents}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>

      <div>
        <label class="text-xs text-warm-sub font-medium">Operation</label>
        <select
          bind:value={op}
          onchange={updateConfig}
          onpointerdown={stopFlowEvents}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
        >
          <option value="upper">upper(col)</option>
          <option value="lower">lower(col)</option>
          <option value="add">col + col/lit</option>
          <option value="concat">concat</option>
        </select>
      </div>

      <div>
        <label class="text-xs text-warm-sub font-medium">Left Column</label>
        <input
          type="text"
          bind:value={left}
          oninput={updateConfig}
          onpointerdown={stopFlowEvents}
          placeholder="e.g. name"
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>

      {#if op === 'add' || op === 'concat'}
        <div>
          <label class="text-xs text-warm-sub font-medium">Right Kind</label>
          <select
            bind:value={rightKind}
            onchange={updateConfig}
            onpointerdown={stopFlowEvents}
            class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
          >
            <option value="column">Column</option>
            <option value="literal">Literal</option>
          </select>
        </div>
        <div>
          <label class="text-xs text-warm-sub font-medium">Right</label>
          <input
            type="text"
            bind:value={right}
            oninput={updateConfig}
            onpointerdown={stopFlowEvents}
            placeholder={rightKind === 'literal' ? 'e.g. _suffix' : 'e.g. other_col'}
            class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
          />
        </div>
      {/if}
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>
</div>
