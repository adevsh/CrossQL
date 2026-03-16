<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';

  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();

  type NullMode = 'keep' | 'drop_row' | 'fill_default' | 'error';

  type SchemaMapColumn = {
    source: string;
    rename: string;
    cast: '' | 'Int64' | 'Float64' | 'Boolean' | 'Utf8' | 'Datetime';
    null_mode: NullMode;
    fill_value: string;
  };

  let columns = $state<SchemaMapColumn[]>([]);

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
    data.config = { columns };
  }

  function addColumn() {
    columns = [
      ...columns,
      { source: '', rename: '', cast: '', null_mode: 'keep', fill_value: '' }
    ];
    updateConfig();
  }

  function removeColumn(idx: number) {
    columns = columns.filter((_, i) => i !== idx);
    updateConfig();
  }

  $effect(() => {
    const nextColumns = Array.isArray(data?.config?.columns) ? (data.config.columns as SchemaMapColumn[]) : [];
    if (columns !== nextColumns) columns = nextColumns;
  });
</script>

<div class="bg-white border-l-4 border-l-[#C49A3C] border border-warm-border rounded shadow-sm w-[28rem]">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">🔀</span>
      <span class="font-bold text-warm-text text-sm">Schema Map</span>
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

    <div class="text-xs text-warm-sub font-medium">Columns</div>

    {#if columns.length === 0}
      <div class="text-xs text-warm-muted">Add columns to rename, cast, and handle nulls.</div>
    {:else}
      <div class="border border-warm-border rounded overflow-hidden">
        <div class="grid grid-cols-12 gap-0 bg-warm-panel text-[11px] text-warm-sub font-semibold border-b border-warm-border">
          <div class="col-span-4 px-2 py-1">Column</div>
          <div class="col-span-3 px-2 py-1">Rename</div>
          <div class="col-span-2 px-2 py-1">Type</div>
          <div class="col-span-2 px-2 py-1">Nulls</div>
          <div class="col-span-1 px-2 py-1"></div>
        </div>

        <div class="max-h-56 overflow-auto">
          {#each columns as c, i (i)}
            <div class="grid grid-cols-12 gap-0 border-b border-warm-border last:border-b-0 bg-white">
              <div class="col-span-4 p-1">
                <input
                  type="text"
                  bind:value={c.source}
                  oninput={updateConfig}
                  onpointerdown={stopFlowEvents}
                  placeholder="source_col"
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                />
              </div>
              <div class="col-span-3 p-1">
                <input
                  type="text"
                  bind:value={c.rename}
                  oninput={updateConfig}
                  onpointerdown={stopFlowEvents}
                  placeholder="optional"
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                />
              </div>
              <div class="col-span-2 p-1">
                <select
                  bind:value={c.cast}
                  onchange={updateConfig}
                  onpointerdown={stopFlowEvents}
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
                >
                  <option value="">Keep</option>
                  <option value="Int64">Int64</option>
                  <option value="Float64">Float64</option>
                  <option value="Boolean">Boolean</option>
                  <option value="Utf8">Utf8</option>
                  <option value="Datetime">Datetime</option>
                </select>
              </div>
              <div class="col-span-2 p-1">
                <select
                  bind:value={c.null_mode}
                  onchange={updateConfig}
                  onpointerdown={stopFlowEvents}
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
                >
                  <option value="keep">Keep</option>
                  <option value="drop_row">Drop row</option>
                  <option value="fill_default">Fill default</option>
                  <option value="error">Error</option>
                </select>
              </div>
              <div class="col-span-1 p-1 flex items-center justify-end">
                <button
                  type="button"
                  onclick={() => removeColumn(i)}
                  onpointerdown={stopFlowEvents}
                  class="text-xs px-2 py-1 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
                  aria-label="Remove"
                >
                  ×
                </button>
              </div>

              {#if c.null_mode === 'fill_default'}
                <div class="col-span-12 px-2 pb-2">
                  <div class="grid grid-cols-12 gap-2 items-center">
                    <div class="col-span-3 text-[11px] text-warm-sub">Fill value</div>
                    <div class="col-span-9">
                      <input
                        type="text"
                        bind:value={c.fill_value}
                        oninput={updateConfig}
                        onpointerdown={stopFlowEvents}
                        placeholder="e.g. 0, true, 2024-01-01T00:00:00Z"
                        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                      />
                    </div>
                  </div>
                </div>
              {/if}
            </div>
          {/each}
        </div>
      </div>
    {/if}

    <button
      type="button"
      onclick={addColumn}
      onpointerdown={stopFlowEvents}
      class="self-start text-xs px-3 py-1.5 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
    >
      Add column
    </button>

    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>
</div>
