<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';

  let { data } = $props();

  let how = $state<'inner' | 'left' | 'outer' | 'cross'>(data.config?.how || 'inner');
  let leftOn = $state<string>(data.config?.left_on || '');
  let rightOn = $state<string>(data.config?.right_on || '');

  function updateConfig() {
    data.config = { how, left_on: leftOn, right_on: rightOn };
  }

  function stopFlowEvents(e: Event) {
    e.stopPropagation();
  }
</script>

<div class="bg-white border-l-4 border-l-[#C07A3A] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center gap-2 bg-warm-bg rounded-t">
    <span class="text-xl">⋈</span>
    <span class="font-bold text-warm-text text-sm">Join</span>
  </div>

  <div class="p-3 flex flex-col gap-3 relative" onpointerdown={stopFlowEvents} onwheel={stopFlowEvents}>
    <Handle id="left" type="target" position={Position.Left} class="!bg-[#C07A3A] !w-3 !h-3 !-left-1.5 !top-9" />
    <Handle id="right" type="target" position={Position.Left} class="!bg-[#C07A3A] !w-3 !h-3 !-left-1.5 !bottom-9" />
    <Handle type="source" position={Position.Right} class="!bg-[#C07A3A] !w-3 !h-3" />

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label class="text-xs text-warm-sub font-medium">Join Type</label>
        <select
          bind:value={how}
          onchange={updateConfig}
          onpointerdown={stopFlowEvents}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
        >
          <option value="inner">Inner</option>
          <option value="left">Left</option>
          <option value="outer">Full Outer</option>
          <option value="cross">Cross</option>
        </select>
      </div>
      <div class="text-[11px] text-warm-muted flex items-end leading-tight">
        Connect inputs to the two left handles.
      </div>
    </div>

    {#if how !== 'cross'}
      <div class="grid grid-cols-2 gap-2">
        <div>
          <label class="text-xs text-warm-sub font-medium">Left Key</label>
          <input
            type="text"
            bind:value={leftOn}
            oninput={updateConfig}
            onpointerdown={stopFlowEvents}
            placeholder="e.g. id"
            class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
          />
        </div>
        <div>
          <label class="text-xs text-warm-sub font-medium">Right Key</label>
          <input
            type="text"
            bind:value={rightOn}
            oninput={updateConfig}
            onpointerdown={stopFlowEvents}
            placeholder="e.g. user_id"
            class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
          />
        </div>
      </div>
    {/if}

    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">
        {data?.stats?.rows_left !== undefined && data?.stats?.rows_right !== undefined
          ? `Rows: ${data.stats.rows_left} + ${data.stats.rows_right} → ${data.stats.rows_out}`
          : `Rows out: ${data.stats.rows_out}`}
      </div>
    {/if}
  </div>
</div>
