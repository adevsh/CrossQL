<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  type Op = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'is_null' | 'is_not_null';
  type ValueType = 'string' | 'number' | 'boolean';

  let column = $state<string>(data.config?.column || '');
  let op = $state<Op>(data.config?.op || 'eq');
  let valueType = $state<ValueType>(data.config?.value_type || 'string');
  let value = $state<string>(data.config?.value || '');

  function updateConfig() {
    data.config = { column, op, value_type: valueType, value };
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
      <span class="text-xl">🔽</span>
      <span class="font-bold text-warm-text text-sm">Filter</span>
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
        <label class="text-xs text-warm-sub font-medium">Column</label>
        <input
          type="text"
          bind:value={column}
          oninput={updateConfig}
          onpointerdown={stopFlowEvents}
          placeholder="e.g. city"
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>

      <div>
        <label class="text-xs text-warm-sub font-medium">Operator</label>
        <select
          bind:value={op}
          onchange={updateConfig}
          onpointerdown={stopFlowEvents}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
        >
          <option value="eq">=</option>
          <option value="ne">!=</option>
          <option value="gt">&gt;</option>
          <option value="gte">&gt;=</option>
          <option value="lt">&lt;</option>
          <option value="lte">&lt;=</option>
          <option value="contains">contains</option>
          <option value="is_null">is null</option>
          <option value="is_not_null">is not null</option>
        </select>
      </div>

      <div>
        <label class="text-xs text-warm-sub font-medium">Value Type</label>
        <select
          bind:value={valueType}
          onchange={updateConfig}
          onpointerdown={stopFlowEvents}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
          disabled={op === 'is_null' || op === 'is_not_null'}
        >
          <option value="string">String</option>
          <option value="number">Number</option>
          <option value="boolean">Boolean</option>
        </select>
      </div>

      <div class="col-span-2">
        <label class="text-xs text-warm-sub font-medium">Value</label>
        <input
          type="text"
          bind:value={value}
          oninput={updateConfig}
          onpointerdown={stopFlowEvents}
          placeholder={op === 'contains' ? 'substring' : 'value'}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
          disabled={op === 'is_null' || op === 'is_not_null'}
        />
      </div>
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>
</div>
